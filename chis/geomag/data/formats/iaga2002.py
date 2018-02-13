'''
IAGA-2002 format

For information regarding this format, please visit:

https://www.ngdc.noaa.gov/IAGA/vdat/IAGA2002/iaga2002format.html

Do note that the following should be called with an obspy.Inventory with the associated station
present in the Inventory.

A note regarding StationXML (Inventory):

The following header information must be present in IAGA2002 format and are deduced as followed:

Source of Data = (hard coded) Geological Survey of Canada
    - this may prove to be a difficulty when converting other institutes
    that are transmitting through GOES and will also be archived under are stream.
    We will have to add them to a custom network code (possibly IN) that will not be public.
    Under the <Station> of StationXML, may be replace with the institutes name
Station name = <Station><Site><Name>
IAGA code = <Station>[code]
latitude = <Station><Latitude>
longitude = <Station><Longitude>
elevation = <Station><Elevation>
reported = FDSN query will query for all available channels for the sampling rate:
    M = 8 hz = query MF?
    L = 1 hz = query LF?
    U = 1/60 hz = query UF?
    There should be a maximum of 4 returned channels for ? == reported
sensor orientation = leave blank for now (like original)
digital sampling = leave blank for now (like original)
data interval = based on sampling rate
    M = 0.125 second
    L = 1 second
    U = 1 minute (00:30-01:29)
Data type = based on location code (first letter)
    R (raw) = variation
    D (definitive) = definitive

Comment with declination base to be hard coded

:author: Charles Blais
'''

import numpy as np
import chis.geomag.data.formats.lib as lib

# contants
DATA_INTERVAL_TYPES = {
    'M' : '0.125 second',
    'L' : '1 second',
    'U' : '1 minute (00:30-01:29)'
}
DATA_TYPES = {
    'R' : 'variation',
    'D' : 'definitive'
}
NULL_VALUE = 99999.00

# pylint: disable=W0613
def write(stream, filename, inventory=None, source=None, **kwargs):
    '''
    :type stream: ~obspy.Stream
    :param stream: Stream containing traces, expected channels are orientation XYZF

    :type filename: str or resource
    :param filename: filename to write too

    :type inventory: ~obspy.Inventory
    :param inventory: Inventory with Station found in stream presetn
    '''

    # If the filename is a resource with write command
    # then its a file resource that we can write directly too
    if not hasattr(filename, "write"):
        file_opened = True
        resource = open(filename, "w")
    else:
        file_opened = False
        resource = filename

    # There should 4 traces in the stream
    if stream.count() != 4:
        raise ValueError("IAGA2002 format must have 4 components")

    # We only support X, Y, Z, F
    # validate required traces
    components = ['X', 'Y', 'Z', 'F']
    for component in components:
        if not stream.select(component=component):
            raise ValueError('Missing trace with component %s' % component)
    stream = lib.order_stream(stream, components=components)

    if not lib.is_common_traces(stream, meta_matches=['network', 'station', 'sampling_rate']):
        raise ValueError(
            "All traces in the stream must come from the same station and sampling rate"
        )

    # At this state, we know all the traces have the same network and station
    # code.  We extract and find the associated inventory object.
    inv = None
    if inventory is not None:
        inv = inventory.select(
            network=stream[0].meta.network,
            station=stream[0].meta.station)

    _write_header(stream, resource, inv, source)
    # Write the body
    _write_body(stream, resource)

    if file_opened:
        resource.close()

def _write_header(stream, resource, inventory, source):
    '''
    Header documentation can be found on top of the file docstring

    Special not to consider:

    Mandatory header and optional comment records begin with a space
    character in column 1 and end with the vertical bar | (ASCII 124)
    in column 70. Content labels begin in column 2 and descriptions
    begin in column 25.

    Example of header:

     Format                 IAGA-2002                                    |
     Source of Data         Geological Survey of Canada (GSC)            |
     Station Name           Ottawa                                       |
     IAGA CODE              OTT                                          |
     Geodetic Latitude      45.403                                       |
     Geodetic Longitude     284.448                                      |
     Elevation              75.000                                       |
     Reported               XYZF                                         |
     Sensor Orientation                                                  |
     Digital Sampling                                                    |
     Data Interval Type     Average 1-Minute(00:30-01:29)                |
     Data Type              variation                                    |
     # DECBAS               000000 (Baseline declination value in        |
     #                      tenths of minutes East (0-216,000)).         |
     # This data file was created by the Ottawa GIN from Reported data.  |
     # Final data will be available on the INTERMAGNET DVD.              |
     # Go to www.intermagnet.org for details on obtaining this product.  |
     # CONDITIONS OF USE: The Conditions of Use for data provided        |
     # through INTERMAGNET and acknowledgement templates can be found    |
     # at www.intermagnet.org                                            |
    '''
    response = _get_headers(stream, inventory, source)
    response.extend([
        ' # DECBAS                000000 (Baseline declination value in       |',
        ' #                       tenths of minutes East (0-216,000)).        |'
    ])
    resource.write('\r\n'.join(response) + '\r\n')


def _get_headers(stream, inventory, source):
    '''See _write_header for information'''

    if source is None:
        source = '' if inventory is None else inventory.source

    # we don't need any network information so lets dumb down the inventory
    # to the station object
    station = inventory.networks[0].stations[0] if inventory else None

    # the reported orientation is the combination of all components in stream channels
    reported = ''.join([trace.meta.channel[-1] for trace in stream]).upper()

    # the data interval type is based on the sampling rate (channel[0])
    data_interval_type = DATA_INTERVAL_TYPES.get(stream[0].meta.channel[0], '')
    # the data type is based on the location[0]
    data_type = DATA_TYPES.get(stream[0].meta.location[0], '')
    return [
        " %-23s %-44s|" % ("Format", "IAGA-2002"),
        " %-23s %-44s|" % ("Source of Data", source),
        " %-23s %-44s|" % ("Station Name", station.site.name),
        " %-23s %-44s|" % ("IAGA CODE", stream[0].meta.station),
        " %-23s %-44s|" % ("Geodetic Latitude", "%.3f" % station.latitude),
        " %-23s %-44s|" % ("Geodetic Longitude", "%.3f" % station.longitude),
        " %-23s %-44s|" % ("Elevation", "%.3f" % station.elevation),
        " %-23s %-44s|" % ("Reported", reported),
        " %-23s %-44s|" % ("Sensor Orientation", ''),
        " %-23s %-44s|" % ("Digital Sampling", ''),
        " %-23s %-44s|" % ("Data Interval Type", data_interval_type),
        " %-23s %-44s|" % ("Data Type", data_type)
    ]


def _write_body(stream, resource):
    '''
    Body of the IAGA-2002 is in the form of:

    DATE       TIME         DOY     OTTX      OTTY      OTTZ      OTTF   |
    2017-11-10 00:00:00.000 314     17845.50  -4328.24  51046.59  54250.70

    We grab the starttime and endtime of all traces and insert them into a trace
    of identical length.  This way, index 0...x are the same time in all traces.
    '''
    # make a copy of the stream before changing it
    # any trace manipulation are done by reference in obspy
    nstream = stream.copy()
    starttime = nstream[0].stats.starttime
    endtime = nstream[0].stats.endtime
    sampling_rate = nstream[0].stats.sampling_rate
    station = nstream[0].stats.station
    components = []
    for trace in nstream:
        starttime = min(trace.meta.starttime, starttime)
        endtime = max(trace.stats.endtime, endtime)
        components.append(trace.meta.channel[-1])
        # for each trace, if its a masked array, convert it to an array
        if np.ma.is_masked(trace):
            trace.data = trace.data.filled(fill_value=NULL_VALUE)

    resource.write(
        "DATE       TIME         DOY     %3s%1s      %3s%1s      %3s%1s      %3s%1s   |\r\n" % (
            station, components[0],
            station, components[1],
            station, components[2],
            station, components[3]
        )
    )

    nstream.trim(starttime, endtime, pad=True, fill_value=NULL_VALUE)

    # print the information by time starting at starttime
    for offset in xrange(len(nstream[0])):
        timestamp = starttime + offset/sampling_rate
        components = []
        for idx in xrange(4):
            components.append(
                nstream[idx][offset] if nstream[idx][offset] else NULL_VALUE
            )
        resource.write("%s %s    %9.2f %9.2f %9.2f %9.2f\r\n" % (
            timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            timestamp.strftime("%j"),
            components[0],
            components[1],
            components[2],
            components[3]
        ))
