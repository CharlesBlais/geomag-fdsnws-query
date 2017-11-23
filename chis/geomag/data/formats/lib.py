'''
:author: Charles Blais
'''

def is_common_traces(stream, meta_matches=None):
    '''
    Verify that the traces in the stream all have the same meta information

    internet
    iaga2002
    imfv122

    :type stream: ~obspy.stream
    :param stream: Stream of data

    :type meta_matches: list
    :param meta_matches: list of meta attributes to match accross all traces

    :return: True or False

    :throws: ValueError
    '''

    if meta_matches is None:
        meta_matches = ['network', 'station']

    prev_match = {}
    for trace in stream:
        for meta_match in meta_matches:
            if meta_match not in prev_match:
                prev_match[meta_match] = getattr(trace.meta, meta_match)
            else:
                if prev_match[meta_match] != getattr(trace.meta, meta_match):
                    return False
    return True
    