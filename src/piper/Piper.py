#############################################################################
# Author: danilevc
# Created on April 08, 2021, 02:10 PM
# Copyright (C) European XFEL GmbH Hamburg. All rights reserved.
#############################################################################
import threading
import time

import numpy as np

from pathlib import Path
from karabo.bound import (PythonDevice, INPUT_CHANNEL, KARABO_CLASSINFO,
                          SLOT_ELEMENT, OUTPUT_CHANNEL, Schema, State, Hash,
                          Epochstamp, Trainstamp, Timestamp, STRING_ELEMENT)

from ._version import version as deviceVersion


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
            .defaultValue('/gpfs/exfel/data/scratch/danilevc/lpd_data_to_validate/Q2M1_cell_8_relgain_only')
            .reconfigurable()
            .commit(),
        )

    def __init__(self, configuration):
        # always call PythonDevice constructor first!
        super().__init__(configuration)
        # Define the first function to be called after the constructor has
        # finished
        self.registerInitialFunction(self.initialization)
        t = threading.Thread(target=self.serve)
        t.start()
        self.saved_count = 0

    def initialization(self):
        """This method will be called after the constructor.

        If you need methods that can be callable from another device or GUI
        you may register them here:
        self.KARABO_SLOT(self.myslot1)
        self.KARABO_SLOT(self.myslot2)
        ...
        Corresponding methods (myslot1, myslot2, ...) should be defined in this
        class
        """
        # Define your slots here
        self.KARABO_SLOT(self.thingy)
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
