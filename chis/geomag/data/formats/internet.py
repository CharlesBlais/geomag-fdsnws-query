'''
Internet format

Example line in the format:

MEA 2017 325:17:39:00 13567.13 3501.27 55422.16 57171.99
where
OBS YYYY DOY:HH:MM:SS XXXXXXXX YYYYYYY ZZZZZZZZ FFFFFFFF

each element are space delimited.abs

Also, the format, in theory, is not day only specific therefore we just do a dump
of the content of the traces from starttime to endtime of all traces.

No StationXML (Inventory) requirements

:author: Charles Blais
'''

import numpy as np
import chis.geomag.data.formats.lib as lib

# constants
NULL_VALUE = 99999.00

# pylint: disable=W0613
def write(stream, filename, **kwargs):
    '''
    Write data in internet format

    The Stream traces must:
        - contain X,Y,Z,F components
        - contain the same network, station and sampling rate

    :type stream: ~obspy.Stream
    :param stream: Stream containing traces, expected channels are orientation XYZF

    :type filename: str or resource
    :param filename: filename to write too
    '''

    # If the filename is a resource with write command
    # then its a file resource that we can write directly too
    if not hasattr(filename, "write"):
        file_opened = True
        resource = open(filename, "w")
    else:
        file_opened = False
        resource = filename

    # validate required traces
    components = ['X', 'Y', 'Z', 'F']
    for component in components:
        if not stream.select(component=component):
            raise ValueError('Missing trace with component %s' % component)

    if not lib.is_common_traces(stream, meta_matches=['network', 'station', 'sampling_rate']):
        raise ValueError(
            "All traces in the stream must come from the same station and sampling rate"
        )

    # Write the body
    _write_body(stream, resource)

    if file_opened:
        resource.close()


def _write_body(stream, resource):
    '''
    Write body of the format

    We grab the starttime and endtime of all traces and insert them into a trace
    of identical length.  This way, index 0...x are the same time in all traces.
    '''
    # make a copy of the stream before changing it
    # any trace manipulation are done by reference in obspy
    nstream = stream.copy()
    # sort stream so that we have XYZF order
    nstream.sort()

    starttime = nstream[0].meta.starttime
    endtime = nstream[0].meta.endtime
    sampling_rate = nstream[0].meta.sampling_rate
    station = nstream[0].meta.station
    for trace in nstream:
        starttime = min(trace.meta.starttime, starttime)
        endtime = max(trace.stats.endtime, endtime)
        # for each trace, if its a masked array, convert it to an array
        if np.ma.is_masked(trace):
            trace.data = trace.data.filled(fill_value=NULL_VALUE)

    nstream.trim(starttime, endtime, pad=True, fill_value=NULL_VALUE)

    # print the information by time starting at starttime
    for offset in xrange(len(nstream[0])):
        timestamp = starttime + offset/sampling_rate
        timestamp = timestamp.strftime("%Y %j:%H:%M:%S") if sampling_rate < 1 else \
            timestamp.strftime("%Y %j:%H:%M:%S.%f")[:-3]
        components = []
        for idx in xrange(4):
            components.append(
                nstream[idx][offset] if nstream[idx][offset] else NULL_VALUE
            )
        resource.write("{station} {timestamp} {X} {Y} {Z} {F}".format(
            station=station,
            timestamp=timestamp,
            X=components[0],
            Y=components[1],
            Z=components[2],
            F=components[3]
        ))
