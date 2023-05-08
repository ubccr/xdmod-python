import pytest
import xdmod.datawarehouse as xdw
import pandas
import numpy
import os

XDMOD_URL = 'https://xdmod-dev.ccr.xdmod.org:9001'
DATA_DIR = os.path.dirname(__file__) + '/data'


@pytest.fixture(scope='module')
def valid_dw():
    with xdw.DataWarehouse(XDMOD_URL) as dw:
        yield dw


def __assert_dfs_equal(
    data_file,
    actual,
    dtype='object',
    index_col='id',
):
    expected = pandas.read_csv(
        DATA_DIR + '/' + data_file,
        dtype=dtype,
        index_col=index_col,
        keep_default_na=False,
        na_values=[''],
    ).fillna(numpy.nan)
    expected.columns = expected.columns.astype('string')
    assert expected.equals(actual)


def test_get_raw_data(valid_dw):
    data = valid_dw.get_raw_data(
        duration=('2021-05-01', '2021-05-02'),
        realm='SUPREMM',
        fields=(
            'CPU User',
            'Nodes',
            'Wall Time',
            'Wait Time',
            'Requested Wall Time',
            'Total memory used',
            'Mount point "home" data written',
            'Mount point "scratch" data written',
        ),
        filters={
            'Resource': [
                'STAMPEDE2 TACC',
                'Bridges 2 RM',
            ],
        },
    ).iloc[::1000]
    data.index = data.index.astype('string')
    __assert_dfs_equal(
        'machine-learning-notebook-example-every-1000.csv',
        data,
        dtype='string',
        index_col=0,
    )


def __assert_descriptor_dfs_equal(data_file, actual):
    __assert_dfs_equal(data_file, actual, 'string')


def test_get_realms(valid_dw):
    __assert_descriptor_dfs_equal(
        'xdmod-dev-realms.csv',
        valid_dw.get_realms(),
    )


def test_get_metrics(valid_dw):
    __assert_descriptor_dfs_equal(
        'xdmod-dev-jobs-metrics.csv',
        valid_dw.get_metrics('Jobs'),
    )


def test_get_dimensions(valid_dw):
    __assert_descriptor_dfs_equal(
        'xdmod-dev-jobs-dimensions.csv',
        valid_dw.get_dimensions('Jobs'),
    )


def test_get_filters(valid_dw):
    __assert_descriptor_dfs_equal(
        'xdmod-dev-jobs-fieldofscience-filters.csv',
        valid_dw.get_filters('Jobs', 'Field of Science'),
    )
