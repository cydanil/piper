#############################################################################
# Author: danilevc
# Created on April 08, 2021, 02:10 PM
# Copyright (C) European XFEL GmbH Hamburg. All rights reserved.
#############################################################################
import threading
import time
from pathlib import Path

import numpy as np
from cycler import cycler
from karabo.bound import (DOUBLE_ELEMENT, IMAGEDATA_ELEMENT, INPUT_CHANNEL,
                          KARABO_CLASSINFO, NODE_ELEMENT, OUTPUT_CHANNEL,
                          SLOT_ELEMENT, STRING_ELEMENT, DaqDataType,
                          Epochstamp, Hash, ImageData, PythonDevice, Schema,
                          State, Timestamp, Trainstamp, Types, Unit)
from processing_utils.rate_calculator import RateCalculator

from ._version import version as deviceVersion

PIXELS = 512


@KARABO_CLASSINFO("Piper", deviceVersion)
class Piper(PythonDevice):

    @staticmethod
    def expectedParameters(expected):
        (
            OUTPUT_CHANNEL(expected).key('output')
            .displayedName('Output')
            .commit(),

            SLOT_ELEMENT(expected).key('thingy')
            .commit(),

            INPUT_CHANNEL(expected).key('input')
            .displayedName('Input')
            # .dataSchema(Schema())
            .commit(),

            STRING_ELEMENT(expected).key('saveTo')
            .assignmentOptional()
            .defaultValue('/gpfs/exfel/data/scratch/danilevc/lpd_data_to_validate/Q2M1_cell_8_relgain_only')  # noqa
            .reconfigurable()
            .commit(),
        )

        triple_output_schema = Schema()
        (
            NODE_ELEMENT(triple_output_schema).key('images')
            .displayedName('images')
            .setDaqDataType(DaqDataType.TRAIN)
            .commit(),

            IMAGEDATA_ELEMENT(triple_output_schema).key('images.raw')
            .displayedName('Raw')
            .setDimensions([PIXELS, PIXELS])
            .setType(Types.UINT32)
            .commit(),

            IMAGEDATA_ELEMENT(triple_output_schema).key('images.gain')
            .displayedName('Gain')
            .setDimensions([PIXELS, PIXELS])
            .setType(Types.UINT8)
            .commit(),

            IMAGEDATA_ELEMENT(triple_output_schema).key('images.corrected')
            .displayedName('Corrected')
            .setDimensions([PIXELS, PIXELS])
            .setType(Types.FLOAT)
            .commit(),

            SLOT_ELEMENT(expected).key('spit3Images')
            .displayedName('Send 3 images')
            .commit(),

            OUTPUT_CHANNEL(expected).key('tripleImages')
            .displayedName('3 Images out')
            .dataSchema(triple_output_schema)
            .commit(),

            DOUBLE_ELEMENT(expected).key('frameRate')
            .displayedName('3 Images rate')
            .unit(Unit.HERTZ)
            .readOnly().initialValue(0.)
            .commit()

        )

    def __init__(self, configuration):
        super().__init__(configuration)
        self.registerInitialFunction(self.initialization)
        self.thread_serve = threading.Thread(target=self.serve)
        self.thread_serve.start()

        self.thread_3out = threading.Thread(target=self.serve_triple_output)
        self.thread_3out.start()

        self.saved_count = 0

    def initialization(self):
        self.KARABO_SLOT(self.thingy)
        self.KARABO_SLOT(self.spit3Images)
        self.updateState(State.NORMAL)
        self.KARABO_ON_DATA("input", self.onData)

    def thingy(self):
        state = State.ACQUIRING if self['state'] == State.NORMAL else State.NORMAL  # noqa
        self.updateState(state)

    def serve(self):
        h = Hash()
        h['data.gain'] = np.zeros((512, 1024))
        h['data.adc'] = np.zeros((512, 1024))
        h['data.passport'] = [self.getInstanceId()]
        h['data.memoryCell'] = b'\x0f'
        h['data.frameNumber'] = 524221
        h['data.trainId'] = 1
        h['data.timestamp'] = 1616169094.918412
        while True:
            if self['state'] == State.ACQUIRING:
                h['data.trainId'] += 1
                timestamp = Timestamp(Epochstamp(),
                                      Trainstamp(h['data.trainId']))
                self.writeChannel('output', h, timestamp)
            else:
                h['data.trainId'] = 1
            time.sleep(0.1)

    def onData(self, data, meta):
        try:
            fname = meta['source'].replace('/', '_').replace(':', '_')
            fname = f"{fname}_{data['image.trainId'][0]}.npy"
            path = Path(self.get('saveTo'))
            path.mkdir(exist_ok=True)
            path = path / fname
            print(path, end=' ')
            np.save(path, data['image.data'][:, :, 0])
            print(f'saved ({self.saved_count})')
        except Exception:
            print(data.keys(), data)
        self.saved_count += 1

    def spit3Images(self):
        state = State.PROCESSING if self['state'] == State.NORMAL else State.NORMAL  # noqa
        self.updateState(state)

    def serve_triple_output(self):
        acq_rate = RateCalculator(refresh_interval=1)
        raw = [np.array([100] * PIXELS * PIXELS, dtype=np.uint32).reshape([PIXELS, PIXELS]),  # noqa
               np.array([200] * PIXELS * PIXELS, dtype=np.uint32).reshape([PIXELS, PIXELS]),  # noqa
               np.array([300] * PIXELS * PIXELS, dtype=np.uint32).reshape([PIXELS, PIXELS])]  # noqa
        gain = [np.zeros([PIXELS, PIXELS], dtype=np.uint8),
                np.ones([PIXELS, PIXELS], dtype=np.uint8),
                np.array([2] * PIXELS * PIXELS, dtype=np.uint8).reshape([PIXELS, PIXELS])]  # noqa
        corrected = [np.array([50] * PIXELS * PIXELS, dtype=np.float32).reshape([PIXELS, PIXELS]),  # noqa
                     np.array([150] * PIXELS * PIXELS, dtype=np.float32).reshape([PIXELS, PIXELS]),  # noqa
                     np.array([250] * PIXELS * PIXELS, dtype=np.float32).reshape([PIXELS, PIXELS])]  # noqa
        cycles = cycler(raw=raw, gain=gain, corrected=corrected)()

        h = Hash()
        while True:
            if self['state'] == State.PROCESSING:
                data = next(cycles)
                h['images.raw'] = ImageData(data['raw'])
                h['images.gain'] = ImageData(data['gain'])
                h['images.corrected'] = ImageData(data['corrected'])
                self.log.INFO(f'{data}\n{h}')
                self.writeChannel('tripleImages', h)

                acq_rate.update()
                rate = acq_rate.refresh()
                if rate is not None:
                    self['frameRate'] = rate
            time.sleep(0.1)

    def __del__(self):
        """Stop the threads."""
        self.thread_serve.done = True
        self.thread_serve.join()
        self.thread_3out.done = True
        self.thread_3out.join()
