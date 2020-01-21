'''
:author: Charles Blais
'''
from pkg_resources import resource_filename

# Third-party library
from obspy import Trace
import numpy as np


def is_common_traces(stream, stats_matches=None):
    '''
    Verify that the traces in the stream all have the same stats information

    internet
    iaga2002
    imfv122

    :type stream: ~obspy.stream
    :param stream: Stream of data

    :type stats_matches: list
    :param stats_matches: list of stats attributes to match accross all traces

    :return: True or False

    :throws: ValueError
    '''

    if stats_matches is None:
        stats_matches = ['network', 'station']

    prev_match = {}
    for trace in stream:
        for stats_match in stats_matches:
            if stats_match not in prev_match:
                prev_match[stats_match] = getattr(trace.stats, stats_match)
            else:
                if prev_match[stats_match] != getattr(trace.stats, stats_match):
                    return False
    return True


def order_stream(stream, components=['X', 'Y', 'Z', 'F']):
    '''
    Order all traces in the stream by the orientation
    '''
    from obspy import Stream

    if len(stream) == 0:
        raise ValueError("We cannot reorder the components of an empty stream object")

    nstream = Stream()
    for component in components:
        tstream = stream.select(component=component)
        if not tstream:
            # Copy the header of another trace and change the component
            # The following assumes the channel is always three characters
            stats = stream[0].stats.copy()
            stats.channel = stats.channel[:-1] + component
            stats.npts = 0
            nstream.append(Trace(np.array([]), header=stats))
        elif len(tstream) != 1:
            raise ValueError("The obspy Stream can not have mutliple identical components.  Recommend merging by component.")
        else:
            nstream += tstream
    return nstream
