import pytest
import xdmod.datawarehouse as xdw
import pandas
import os

XDMOD_URL = 'https://xdmod-dev.ccr.xdmod.org:9001'
API_KEY = os.getenv('API_KEY')
DATA_DIR = os.path.dirname(__file__) + '/data'


@pytest.fixture(scope='module')
def valid_dw():
    with xdw.DataWarehouse(XDMOD_URL, API_KEY) as dw:
        yield dw


def __assert_dfs_equal(data_file, actual, index_col='id', string_index=False):
    expected = pandas.read_csv(
        DATA_DIR + '/' + data_file,
        dtype='object',
        index_col=index_col,
    )
    if string_index:
        expected.index = expected.index.astype('string').astype('object')
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
    __assert_dfs_equal(
        'machine-learning-notebook-example-every-1000.csv',
        data,
        index_col=0,
    )


def test_get_realms(valid_dw):
    __assert_dfs_equal(
        'xdmod-dev-realms.csv',
        valid_dw.get_realms(),
    )


def test_get_metrics(valid_dw):
    __assert_dfs_equal(
        'xdmod-dev-jobs-metrics.csv',
        valid_dw.get_metrics('Jobs'),
    )


def test_get_dimensions(valid_dw):
    __assert_dfs_equal(
        'xdmod-dev-jobs-dimensions.csv',
        valid_dw.get_dimensions('Jobs'),
    )


def test_get_filters(valid_dw):
    __assert_dfs_equal(
        'xdmod-dev-jobs-fieldofscience-filters.csv',
        valid_dw.get_filters('Jobs', 'Field of Science'),
        string_index=True,
    )
