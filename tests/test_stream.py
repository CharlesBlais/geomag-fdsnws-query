'''
..  codeauthor:: Charles Blais
'''

# Third-party library
import pytest
from obspy import Trace
import numpy as np

# User-contributed library
import pygeomag.data.stream


def test_merge_by_location():
    '''
    The the merge by location by creating a trace for two different location
    '''
    stream = pygeomag.data.stream.Stream([
        Trace(
            np.array([1, 2, 3]),
            header={'station': 'OTT', 'location': 'R1', 'channel': 'UFX'}),
        Trace(
            np.array([1, 2, 4]),
            header={'station': 'OTT', 'location': 'R2', 'channel': 'UFX'})
    ])
    nstream = stream.merge_by_location()
    assert len(nstream) == 1
    assert nstream[0].data[2] == 3


def test_merge_by_location_reorder():
    '''
    Same as above but the order of the Trace in the stream should not change the result
    '''
    stream = pygeomag.data.stream.Stream([
        Trace(
            np.array([1, 2, 4]),
            header={'station': 'OTT', 'location': 'R2', 'channel': 'UFX'}),
        Trace(
            np.array([1, 2, 3]),
            header={'station': 'OTT', 'location': 'R1', 'channel': 'UFX'})
    ])
    nstream = stream.merge_by_location()
    assert len(nstream) == 1
    assert nstream[0].data[2] == 3


def test_merge_by_location_missing():
    '''
    Same as above but the order of the Trace in the stream should not change the result
    '''
    stream = pygeomag.data.stream.Stream([
        Trace(
            np.array([1, 2, 4], dtype=np.float32),
            header={'station': 'OTT', 'location': 'R2', 'channel': 'UFX'}),
        Trace(
            np.ma.array([1, 2, None], mask=[False, False, True], dtype=np.float32),
            header={'station': 'OTT', 'location': 'R1', 'channel': 'UFX'})
    ])
    print(stream)
    nstream = stream.merge_by_location()
    assert len(nstream) == 1
    assert nstream[0].data[2] == 4


def test_merge_by_location_diff():
    '''
    Different station should not merge since its only location
    '''
    stream = pygeomag.data.stream.Stream([
        Trace(
            np.array([1, 2, 4], dtype=np.float32),
            header={'station': 'OTT', 'location': 'R2', 'channel': 'UFX'}),
        Trace(
            np.ma.array([1, 2, None], mask=[False, False, True], dtype=np.float32),
            header={'station': 'SNK', 'location': 'R1', 'channel': 'UFX'})
    ])
    nstream = stream.merge_by_location()
    assert len(nstream) == 2


def test_merge_by_location_order():
    '''
    Do a merge by defining the order manually.  In this case, I won't R2 to the
    precendence over R1.
    '''
    stream = pygeomag.data.stream.Stream([
        Trace(
            np.array([1, 2, 3]),
            header={'station': 'OTT', 'location': 'R1', 'channel': 'UFX'}),
        Trace(
            np.array([1, 2, 4]),
            header={'station': 'OTT', 'location': 'R2', 'channel': 'UFX'})
    ])
    nstream = stream.merge_by_location(locations=['R2', 'R1'])
    assert len(nstream) == 1
    assert nstream[0].data[2] == 4
