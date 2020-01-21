'''
IMFv1.22 format
===============

Only minute variation data to be used.
    location code of R?
    sampling code of U

..  note::
    We only support XYZF and we don't bother supporting others in IMFv1.22 conversion
    Format should be deprecated so minimal support.

Example of header:
OTT NOV0117 305 00 XYZF R OTT 04462844 000000 RRRRRRRRRRRRRRRR

OTT = <Station>[code]
XYZF = as with IAGA2002 reported field, based on the return response
R = hard coded (reported = variation)
0446 = (90 - <Station><Latitude>) * 10 = colatitude
2844 = <Station><Longitude> * 10 = longitude
000000 RRRRRRRRRRRRRRRR = hard coded

..  note::
    We assume the query was made with day data only (does not expand beyond the day)

..  codeauthor:: Charles Blais
'''

import numpy as np
from obspy import UTCDateTime
import pygeomag.data.formats.lib as lib

# constants
MONTHS_STR = [
    'JAN', 'FEB', 'MAR', 'APR',
    'MAY', 'JUN', 'JUL', 'AUG',
    'SEP', 'OCT', 'NOV', 'DEC'
]
NULL_VALUE = 99999.99
COMPONENTS = ['X', 'Y', 'Z', 'F']


def get_filename(stats):
    '''
    Get the IMFv1.22 approved filename according to the stats of a trace.
    Data type is determined by the location code.

    :type stats: :class:`obspy.Stats`
    :return: filename
    '''
    return "{monthstr}{datetime}.{station}".format(
        monthstr=MONTHS_STR[stats.starttime.month-1],
        datetime=stats.starttime.strftime("%d%y"),
        station=stats.station.upper()
    )


def write(stream, filename, inventory=None, **kwargs):
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

    if not lib.is_common_traces(stream, stats_matches=['network', 'station', 'sampling_rate']):
        raise ValueError(
            "All traces in the stream must come from the same station and sampling rate"
        )

    stream = lib.order_stream(stream, components=COMPONENTS)

    # Only minute variation data is supported in IMFv122
    if stream[0].stats.delta != 60.0:
        raise ValueError('Only minute data is supported in IMFv1.22 format')

    # At this state, we know all the traces have the same network and station
    # code.  We extract and find the associated inventory object.
    inv = None
    if inventory is not None:
        inv = inventory.select(
            network=stream[0].stats.network,
            station=stream[0].stats.station)

    # Write the body
    _write_body(stream, resource, inv)

    if file_opened:
        resource.close()


def _write_body(stream, resource, inventory):
    '''
    IMFv1.22 body format is as followed

    A hourly header is repeated at each hour:
    OTT NOV0117 305 00 XYZF R OTT 04462844 000000 RRRRRRRRRRRRRRRR

    Each hour is block of 30 lines (2 rows of XYZF entries) of values in the form

     178538  -43210  510630 542663   178541  -43210  510630 542665
     178544  -43206  510630 542665   178537  -43203  510629 542661

    These are nT*10 in the form:
    %7d %7d %7d %6d  %7d %7d %7d %6d
    '''

    # make a copy of the stream before changing it
    # any trace manipulation are done by reference in obspy
    nstream = stream.copy()
    for trace in nstream:
        # for each trace, if its a masked array, convert it to an array
        if np.ma.is_masked(trace):
            trace.data = trace.data.filled(fill_value=NULL_VALUE)

    station_code = nstream[0].stats.station
    # we don't need any network information so lets dumb down the inventory
    # to the station object
    station = inventory.networks[0].stations[0] if inventory else None
    colatitude10 = (90 - station.latitude) * 10 if station else 0
    longitude10 = station.longitude * 10 if station else 0

    # The starttime is the begining of the day in the stream
    # IAGA2002 files are always daily files
    starttime = UTCDateTime(nstream[0].stats.starttime.date)
    endtime = starttime + 86400.0 - nstream[0].stats.delta
    nstream.trim(starttime, endtime, pad=True, fill_value=NULL_VALUE)

    if len(nstream[0]) != 1440:
        raise ValueError("Error, the trace does not contain a full day worth of data")

    date = MONTHS_STR[starttime.month-1] + starttime.strftime("%d%y")
    doy = starttime.strftime("%j")

    # for each hour block, add a header
    for hour in range(24):
        resource.write(
            "%3s %s %s %02d XYZF R OTT %04d%04d 000000 RRRRRRRRRRRRRRRR\n" % (
                station_code,
                date, doy, hour,
                colatitude10, longitude10
            )
        )
        for minute in range(60):
            idx = hour*60 + minute
            resource.write("%7d %7d %7d %6d" % (
                (nstream[0].data[idx] if nstream[0].data[idx] < NULL_VALUE else NULL_VALUE)*10,
                (nstream[1].data[idx] if nstream[1].data[idx] < NULL_VALUE else NULL_VALUE)*10,
                (nstream[2].data[idx] if nstream[2].data[idx] < NULL_VALUE else NULL_VALUE)*10,
                (nstream[3].data[idx] if nstream[3].data[idx] < NULL_VALUE else NULL_VALUE)*10,
            ))
            resource.write("\n" if minute % 2 else "  ")
