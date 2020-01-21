'''
..  codeauthor:: Charles Blais
'''
# Third-party library
import pytest
from obspy import UTCDateTime
from obspy.core.trace import Stats

# User-contributed library
import pygeomag.data.formats.iaga2002
import pygeomag.data.formats.imfv122


def test_iaga2002_filename():
    '''
    Test generating IAGA2002 filename
    '''
    stats = Stats(header={
        'network': 'C2',
        'station': 'OTT',
        'location': 'R0',
        'channel': 'UFX',
        'starttime': UTCDateTime(2020, 1, 10),
        'detla': 60
    })
    assert pygeomag.data.formats.iaga2002.get_filename(stats) == 'ott20200110vmin.min'


def test_imfv122_filename():
    '''
    Test generating IAGA2002 filename
    '''
    stats = Stats(header={
        'network': 'C2',
        'station': 'OTT',
        'location': 'R0',
        'channel': 'UFX',
        'starttime': UTCDateTime(2020, 1, 10),
        'detla': 60
    })
    assert pygeomag.data.formats.imfv122.get_filename(stats) == 'JAN1020.OTT'
