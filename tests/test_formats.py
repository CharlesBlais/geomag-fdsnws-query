'''
..  codeauthor:: Charles Blais
'''

import os
import io

# Third-party library
import pytest
import numpy as np
from obspy import read, Stream, Trace, UTCDateTime

# User-contributed library
import pygeomag.data.stream

# Constants
REAL_DATA_STARTTIME = UTCDateTime(2020, 1, 19, 0, 0, 0)
REAL_DATA_ENDTIME = UTCDateTime(2020, 1, 19, 23, 59, 0)


@pytest.fixture
def data():
    return pygeomag.data.stream.Stream(read(os.path.join("tests", "example", "20200119.C2.OTT.mseed")))


@pytest.fixture
def test_data():
    '''
    Simple test data set with 3 values at 0
    '''
    return pygeomag.data.stream.Stream(Stream([
        Trace(
            np.array([1., 2., 3.]),
            header={
                'network': 'C2', 'station': 'OTT', 'location': 'R1', 'channel': 'UFX',
                'delta': 60, 'starttime': UTCDateTime(2019, 1, 2, 0, 0, 0)
            }
        )
    ]))


@pytest.fixture
def test_data_begin_missing():
    '''
    Simple test data set with 3 values at minute 1
    '''
    return pygeomag.data.stream.Stream(Stream([
        Trace(
            np.array([1., 2., 3.]),
            header={
                'network': 'C2', 'station': 'OTT', 'location': 'R1', 'channel': 'UFX',
                'delta': 60, 'starttime': UTCDateTime(2019, 1, 2, 0, 1, 0)
            }
        )
    ]))


@pytest.fixture
def test_data_duplicate():
    '''
    Simple test data set with 3 values at minute 1 but with identical
    component at different location.  All the format libraries only support
    a single trace by component.
    '''
    return pygeomag.data.stream.Stream(Stream([
        Trace(
            np.array([1., 2., 3.]),
            header={
                'network': 'C2', 'station': 'OTT', 'location': 'R1', 'channel': 'UFX',
                'delta': 60, 'starttime': UTCDateTime(2019, 1, 2, 0, 1, 0)
            }
        ),
        Trace(
            np.array([2., 3.]),
            header={
                'network': 'C2', 'station': 'OTT', 'location': 'R2', 'channel': 'UFX',
                'delta': 60, 'starttime': UTCDateTime(2019, 1, 2, 0, 2, 0)
            }
        )
    ]))


@pytest.fixture
def test_data_out_of_range():
    '''
    A test data set where the input streams are not for the same date
    This should return an error for output formats.
    '''
    return pygeomag.data.stream.Stream(Stream([
        Trace(
            np.array([1., 2., 3.]),
            header={
                'network': 'C2', 'station': 'OTT', 'location': 'R1', 'channel': 'UFX',
                'delta': 60, 'starttime': UTCDateTime(2019, 1, 2, 0, 0, 0)
            }
        ),
        Trace(
            np.array([1., 2., 3.]),
            header={
                'network': 'C2', 'station': 'OTT', 'location': 'R1', 'channel': 'UFY',
                'delta': 60, 'starttime': UTCDateTime(2019, 1, 3, 0, 0, 0)
            }
        )
    ]))


def test_iaga2002(test_data):
    '''
    Test the IAGA2002 data
    '''
    buffer = io.StringIO()
    test_data.write(buffer, format='IAGA2002')
    buffer.seek(0)
    content = buffer.getvalue()
    print(content)
    # The last line of the file should our last sample of the test data
    assert "2019-01-02 00:02:00.000 002         3.00  99999.00  99999.00  99999.00\r\n" in content


def test_iaga2002_with_data_no_merge(data):
    '''
    Test the IAGA2002 data
    '''
    with pytest.raises(ValueError) as excinfo:
        buffer = io.StringIO()
        data.write(buffer, format='IAGA2002')
    assert "mutliple identical components" in str(excinfo.value)


def test_iaga2002_with_data(data):
    '''
    Test the IAGA2002 data
    A real data set may contain several locations therefore we need to merge by location
    before writting
    '''
    # clean the data
    cdata = data.merge_by_location().trim(REAL_DATA_STARTTIME, REAL_DATA_ENDTIME)

    buffer = io.StringIO()
    cdata.write(buffer, format='IAGA2002')
    buffer.seek(0)
    content = buffer.getvalue()
    print(content[0:10000])
    # The last line of the file should our last sample of the test data
    assert "2020-01-19 00:00:00.000 019     17208.00  -4902.70  49973.90  53270.80\r\n" in content


def test_iaga2002_begin_missing(test_data_begin_missing):
    '''
    Test the IAGA2002 data
    '''
    buffer = io.StringIO()
    test_data_begin_missing.write(buffer, format='IAGA2002')
    buffer.seek(0)
    content = buffer.getvalue()
    print(content)
    # The last line of the file should our last sample of the test data
    assert "2019-01-02 00:00:00.000 002     99999.00  99999.00  99999.00  99999.00\r\n" in content
    assert "2019-01-02 00:03:00.000 002         3.00  99999.00  99999.00  99999.00\r\n" in content


def test_iaga2002_duplicate(test_data_duplicate):
    '''
    Test the IAGA2002 data
    '''
    with pytest.raises(ValueError) as excinfo:
        buffer = io.StringIO()
        test_data_duplicate.write(buffer, format='IAGA2002')
    assert "mutliple identical components" in str(excinfo.value)


def test_iaga2002_out_of_range(test_data_out_of_range):
    '''
    Test the IAGA2002 data
    '''
    with pytest.raises(ValueError) as excinfo:
        buffer = io.StringIO()
        test_data_out_of_range.write(buffer, format='IAGA2002')
    assert "does not contain data for the same day" in str(excinfo.value)


def test_imfv122(test_data):
    '''
    Test the IMFv1.22 data
    '''
    buffer = io.StringIO()
    test_data.write(buffer, format='imfv122')
    buffer.seek(0)
    content = buffer.getvalue()
    print(content)
    # The last line of the file should our last sample of the test data
    assert "     10  999999  999999 999999       20  999999  999999 999999\n" in content


def test_imfv122_begin_missing(test_data_begin_missing):
    '''
    Test the IMFv1.22 data
    '''
    buffer = io.StringIO()
    test_data_begin_missing.write(buffer, format='imfv122')
    buffer.seek(0)
    content = buffer.getvalue()
    print(content)
    # The last line of the file should our last sample of the test data
    assert " 999999  999999  999999 999999       10  999999  999999 999999\n" in content
    assert "     20  999999  999999 999999       30  999999  999999 999999\n" in content


def test_imfv122_duplicate(test_data_duplicate):
    '''
    Test the IMFv1.22 data
    '''
    with pytest.raises(ValueError) as excinfo:
        buffer = io.StringIO()
        test_data_duplicate.write(buffer, format='imfv122')
    assert "mutliple identical components" in str(excinfo.value)
