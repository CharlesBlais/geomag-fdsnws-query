#!/usr/bin/env python
'''
Query the FDSN-WS for geomagnetic data and convert to geomagnetic standard formats.

Geomagnetic data standard are in daily format so queries are limited per day (--date).

It is important to note that some geomagnetic data formats contain stats information about
the stations.  The program may query the FDSN-WS for the StationXML response or get the
information from a file.  It is highly recommended to include a file for certain formats
as their output result may be incompatible.

For more information on each format, look under:
    pygeomag/data/formats/

..  codeauthor:: Charles Blais
'''
# input arguments
import argparse
import logging
import datetime
import sys
import os
import pathlib

# Client required to query FDSN-WS for conversion
from obspy.clients.fdsn.client import Client
from obspy import UTCDateTime

# For parsing datetime (smart)
import dateutil

# User-contributed library
from pygeomag.data.stream import Stream
# used for generating filenames
import pygeomag.data.formats.iaga2002
import pygeomag.data.formats.imfv122

# Constants
DEFAULT_DATE = datetime.datetime.now().strftime("%Y-%m-%d")
DEFAULT_DIRECTORY = os.getcwd()
DEFAULT_FDNWS = 'http://fdsn.seismo.nrcan.gc.ca/'
DEFAULT_NETWORK = 'C2'
DEFAULT_LOCATIONS = ['R?']
DEFAULT_CHANNELS = ['UFX', 'UFY', 'UFZ', 'UFF']


def fdsnws2geomag():
    '''Convert fdsnws query to geomagnetic data file'''
    parser = argparse.ArgumentParser(
        description='Query the FDSN webservice and convert the geomagnetic data standards')
    parser.add_argument(
        '--url',
        default=DEFAULT_FDNWS,
        help='FDSN-WS URL (default: %s)' % DEFAULT_FDNWS)
    parser.add_argument(
        '--format',
        choices=['internet', 'iaga2002', 'imfv122'],
        default='iaga2002',
        help="Output format (default: iaga2002)")
    parser.add_argument(
        '--output',
        default=sys.stdout,
        help='Output file (default: stdout).')
    # query specific parameters
    parser.add_argument(
        '--date',
        default=DEFAULT_DATE,
        help='Date of the request (default: %s)' % DEFAULT_DATE)
    parser.add_argument(
        '--network',
        default=DEFAULT_NETWORK,
        help='Network code (default: DEFAULT_NETWORK)')
    parser.add_argument(
        '--station',
        required=True,
        help='Station code')
    parser.add_argument(
        '--location',
        nargs='+',
        default=DEFAULT_LOCATIONS,
        help='Data type + source (data type = R - raw, D - definitive, source = 0,1,2,3..., default: %s)' % DEFAULT_LOCATIONS)
    parser.add_argument(
        '--channel',
        nargs='+',
        default=DEFAULT_CHANNELS,
        help='FDSN compliant channel query (default: %s)' % ",".join(DEFAULT_CHANNELS))
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbosity')
    args = parser.parse_args()

    # Set the logging level
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)s \
            %(module)s %(funcName)s: %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO if args.verbose else logging.WARNING)

    # Convert date to starttime and endtime
    reftime = UTCDateTime(args.date)
    starttime = UTCDateTime(reftime.datetime.replace(hour=0, minute=0, second=0, microsecond=0))
    endtime = UTCDateTime(reftime.datetime.replace(hour=23, minute=59, second=59, microsecond=999999))

    # Create a handler client
    logging.info("Connecting to %s", args.url)
    client = Client(args.url)
    logging.info(
        "Requesting data for %s.%s.%s.%s from %s to %s",
        args.network, args.station, ",".join(args.location), ",".join(args.channel),
        starttime.isoformat(), endtime.isoformat())
    stream = Stream(client.get_waveforms(
        args.network, args.station, ",".join(args.location), ",".join(args.channel),
        starttime, endtime))
    logging.info("Found stream: %s", str(stream.__str__(extended=True)))
    # Load optional inventory information
    inventory = client.get_stations(network=args.network, station=args.station)

    # Handle if no data was found
    if not stream:
        logging.warning("No data found")
        return 1

    # Before sending the raw data for writing, we need to trim the response
    # from the FDSNWS query to are actual request time.  We also merge by
    # location.
    logging.info("Writing informtion to %s", str(args.output))
    stream.merge_by_location().trim(starttime, endtime).write(
        args.output,
        format=args.format,
        inventory=inventory
    )


def fdsnws2directory():
    '''
    Much like the fdsnws2geomag but is purely design to get the data from the FDSN-WS
    and add it according to the structure found on geomagnetic daqs servers.

    These structure vary depending on the source but can be customized by input argument.

    Filename for each can not be customized since these following strict naming convention.

    The convention can be found in the pygeomag/data/formats directory.
    '''
    parser = argparse.ArgumentParser(
        description='Query the FDSN webservice and convert the geomagnetic data standards')
    parser.add_argument(
        '--url',
        default=DEFAULT_FDNWS,
        help='FDSN-WS URL (default: %s)' % DEFAULT_FDNWS)
    parser.add_argument(
        '--format',
        choices=['iaga2002', 'imfv122'],
        default='iaga2002',
        help="Output format (default: iaga2002)")
    parser.add_argument(
        '--directory',
        default=DEFAULT_DIRECTORY,
        help='Output directory with optional datetime parameter as accept by python datetime (default: %s).' % DEFAULT_DIRECTORY)
    # query specific parameters
    parser.add_argument(
        '--date',
        default=DEFAULT_DATE,
        help='Date of the request (default: %s)' % DEFAULT_DATE)
    parser.add_argument(
        '--network',
        default=DEFAULT_NETWORK,
        help='Network code (default: DEFAULT_NETWORK)')
    parser.add_argument(
        '--station',
        default='*',
        help='Station code (default: *)')
    parser.add_argument(
        '--location',
        nargs='+',
        default=DEFAULT_LOCATIONS,
        help='Data type + source (data type = R - raw, D - definitive, source = 0,1,2,3..., default: %s)' % DEFAULT_LOCATIONS)
    parser.add_argument(
        '--channel',
        nargs='+',
        default=DEFAULT_CHANNELS,
        help='FDSN compliant channel query (default: %s)' % "," % DEFAULT_CHANNELS)
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbosity')
    args = parser.parse_args()

    # Set the logging level
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)s \
            %(module)s %(funcName)s: %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO if args.verbose else logging.WARNING)

    # Convert date to starttime and endtime
    reftime = UTCDateTime(args.date)
    starttime = UTCDateTime(reftime.datetime.replace(hour=0, minute=0, second=0, microsecond=0))
    endtime = UTCDateTime(reftime.datetime.replace(hour=23, minute=59, second=59, microsecond=999999))

    # Create a handler client
    logging.info("Connecting to %s", args.url)
    client = Client(args.url)
    logging.info(
        "Requesting data for %s.%s.%s.%s from %s to %s",
        args.network, args.station, ",".join(args.location), ",".join(args.channel),
        starttime.isoformat(), endtime.isoformat())
    stream = Stream(client.get_waveforms(
        args.network, args.station, ",".join(args.location), ",".join(args.channel),
        starttime, endtime))
    logging.info("Found stream: %s", str(stream.__str__(extended=True)))
    # Load optional inventory information
    inventory = client.get_stations(network=args.network, station=args.station)

    # Handle if no data was found
    if not stream:
        logging.warning("No data found")
        return 1

    # Before sending the raw data for writing, we need to trim the response
    # from the FDSNWS query to are actual request time.  We also merge by
    # location.
    stream = stream.merge_by_location().trim(starttime, endtime)

    # Loop through the list of stream and generate the unique list of station
    # codes.  We know the network code is constant and its a single sampling rate
    # request.
    stations = set([trace.stats.station for trace in stream])

    # Convert the directory format string to a full path
    directory = starttime.strftime(args.directory)
    logging.info("Creating directory %s if does not exist", directory)
    pathlib.Path(directory).mkdir(parents=True, exist_ok=True)

    for station in stations:
        # Extract the station I need
        extract = stream.select(station=station)
        # Generate its filename (depends on the format)
        if args.format in ['iaga2002']:
            filename = pygeomag.data.formats.iaga2002.get_filename(extract[0].stats)
        elif args.format in ['imfv122']:
            filename = pygeomag.data.formats.imfv122.get_filename(extract[0].stats)
        else:
            raise ValueError("Unable to generate filename for unhandled format %s" % args.format)
        filename = os.path.join(directory, filename)
        logging.info("Writing magnetic data to %s", filename)
        extract.write(
            filename,
            format=args.format,
            inventory=inventory
        )
