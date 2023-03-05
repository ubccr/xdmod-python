import pytest
import xdmod.datawarehouse as xdw
import pandas
import os

XDMOD_URL = 'https://xdmod-dev.ccr.xdmod.org:9001'
DATA_DIR = os.path.dirname(__file__) + '/data'


@pytest.fixture(scope='module')
def valid_dw():
    with xdw.DataWarehouse(XDMOD_URL) as dw:
        yield dw


def __assert_dfs_equal(data_file, actual):
    expected = pandas.read_csv(
        DATA_DIR + '/' + data_file, dtype='object'
    )
    expected = expected.set_index('id')
    assert expected.equals(actual)


def test_get_realms(valid_dw):
    __assert_dfs_equal(
        'xdmod-dev-realms.csv',
        valid_dw.get_realms()
    )


def test_get_metrics(valid_dw):
    __assert_dfs_equal(
        'xdmod-dev-jobs-metrics.csv',
        valid_dw.get_metrics('Jobs')
    )


def test_get_dimensions(valid_dw):
    __assert_dfs_equal(
        'xdmod-dev-jobs-dimensions.csv',
        valid_dw.get_dimensions('Jobs')
    )


def test_get_filters(valid_dw):
    __assert_dfs_equal(
        'xdmod-dev-jobs-fieldofscience-filters.csv',
        valid_dw.get_filters('Jobs', 'Field of Science')
    )
