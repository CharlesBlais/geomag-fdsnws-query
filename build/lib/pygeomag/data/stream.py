'''
Magnetic data stream
====================

Wrapper for an obspy stream that handles writing in geomagnetic formats.

Supported formats are listed in the formats directors.  These include:

- iaga2002
- imfv122
- internet

..  codeauthor:: Charles Blais
'''

import os
import copy
import importlib
from obspy import Stream as ObspyStream


class Stream(ObspyStream):
    '''
    Overwrite Stream write routine to add additional formats
    '''
    def write(self, filename, **kwargs):
        '''
        Write information from geomag database to geomag specified format

        See write routine of formats for list of keywords.
        All write routines take this object as first argument and then
        the filename and keyword.
        '''
        try:
            write_format = importlib.import_module(
                'pygeomag.data.formats.%s' % kwargs.get('format', 'iaga2002').lower()
            )
            return write_format.write(self, filename, **kwargs)
        except ImportError:
            super(Stream, self).write(self, filename, **kwargs)

    def merge_by_location(self, locations=None, replace_location=''):
        '''
        We reorder the traces in the stream by renaming the location code
        with a numeric value and then merging the streams.

        The trace with the ascending name result be taken last in the merge.
        Obspy merge=1 will take the last trace in the stream as the final result.

        Not specifying the location results in a merging by ascending location
        code.
        '''
        if locations is not None:
            # if we specify location codes to order, we extract them
            # and rename them
            new_stream = Stream()
            for idx in range(len(locations)):
                temp_stream = self.select(location=locations[idx])
                for trace in temp_stream:
                    trace.stats.location = "%02d" % idx
                new_stream += temp_stream
        else:
            new_stream = self.copy()

        # Reorder in reverse order (but will result in a ascending merge with
        # method 1).
        new_stream.sort(keys=['location'])
        # Erase the location code from all the streams
        for trace in new_stream:
            trace.stats.location = replace_location
        return new_stream.merge(method=1)
