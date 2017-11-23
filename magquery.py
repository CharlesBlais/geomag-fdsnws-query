#!/usr/bin/env python
'''
Query the FDSN-WS for geomagnetic data and convert to geomagnetic standard formats.

Geomagnetic data standard are in daily format so queries are limited per day (--date).

It is important to note that some geomagnetic data formats contain meta information about
the stations.  The program may query the FDSN-WS for the StationXML response or get the
information from a file.  It is highly recommended to include a file for certain formats
as their output result may be incompatible.

For more information on each format, look under:
    chis/geomag/data/formats/

:author: Charles Blais
'''

# input arguments
import argparse

# Client required to query FDSN-WS for conversion
from obspy.clients.fdsn.client import Client
from obspy import UTCDateTime
from obspy import read_inventory
from chis.geomag.data.Stream import Stream

def query(**kwargs):
    '''
    Query the FDSN-WS for magnetometer data

    :param url: fdsn base URL
    :param date: date of the query
    :param network: network code
    :param station: station code
    :param location: location code
    :param samplingrate: sampling rate code
    :param merge_locations: merge all locations together
        (this may alter the location from the response if they are different)
        (ex: R0 and R1 = R?, R0 and D = ?0)
    '''
    clt = Client(kwargs.get('url'))
    starttime = UTCDateTime(kwargs.get('date'))
    endtime = starttime + 86400.0
    stream = clt.get_waveforms(
        kwargs.get('network'),
        kwargs.get('station'),
        kwargs.get('location'),
        kwargs.get('channel'),
        starttime,
        endtime)
    # add our custom write routine to the default obspy write routine
    stream.write = Stream.write

    if kwargs.get('merge_locations'):
        # sort the stream by location code so that the lowest source is taken as reference
        stream.sort(keys=['location'], reverse=True)
        # we merge all common channel codes together into a single trace
        # we make all location codes identical from the traces for the merge to ignore data
        # types and sources
        new_location = stream[0].meta.location
        for trace in stream:
            for idx in xrange(len(trace.meta.location)):
                if trace.meta.location[idx] != new_location[idx]:
                    new_location[idx] = '?'
        for trace in stream:
            trace.meta.location = new_location
        return stream.merge(method=1)
    return stream

def main():
    '''Main program call'''
    import sys
    parser = argparse.ArgumentParser(
        description='Query the FDSN webservice and convert the geomagnetic data standards')
    parser.add_argument(
        '--url',
        choices=['http://antarc-o2.seismo.nrcan.gc.ca:8080'],
        default='http://antarc-o2.seismo.nrcan.gc.ca:8080',
        help='FDSN-WS URL')
    parser.add_argument(
        '--format',
        choices=['internet', 'iaga2002', 'imfv122'],
        default='IAGA2002',
        help="Output format")
    parser.add_argument(
        '--outfile',
        default=sys.stdout,
        help='Output file (default: stdout).')
    parser.add_argument(
        '--stationxml',
        default='stations.xml',
        help='StationXML configuration file if not taken from FDSN')

    # query specific parameters
    parser.add_argument(
        '--date',
        default=UTCDateTime().now().strftime('%Y-%m-%d'),
        help='Date of the request')
    parser.add_argument(
        '--network',
        default='C2',
        help='Network code (default: C2)')
    parser.add_argument(
        '--station',
        required=True,
        help='Station code')
    parser.add_argument(
        '--location',
        default='R?',
        help='Data type + source (data type = R - raw, D - definitive, source = 1,2,3...)')
    parser.add_argument(
        '--channel',
        default='UFX,UFY,UFZ,UFF',
        help='FDSN compliant channel query (default: UFX,UFY,UFZ,UFF')
    args = parser.parse_args()

    print args
    data = query(merge_locations=True, **vars(args))
    data.write(
        args.outfile,
        format=args.format,
        inventory=read_inventory(args.stationxml)
    )

if __name__ == "__main__":
    main()
