import os
import pandas
import pytest
import xdmod.datawarehouse as xdw


class TestDataWarehouse:
    __DATA_DIR = os.path.dirname(__file__) + '/data'
    __XDMOD_URL = 'https://xdmod-dev.ccr.xdmod.org'

    @pytest.fixture
    def valid_dw(self):
        yield xdw.DataWarehouse(self.__XDMOD_URL)

    def __assert_dfs_equal(self, data_file, actual):
        expected = pandas.read_csv(
            self.__DATA_DIR + '/' + data_file, dtype='object'
        )
        expected = expected.set_index('id')
        assert expected.equals(actual)

    def test_get_realms(self, valid_dw):
        with valid_dw:
            self.__assert_dfs_equal(
                'xdmod-dev-realms.csv', valid_dw.get_realms()
            )

    def test_get_metrics(self, valid_dw):
        with valid_dw:
            self.__assert_dfs_equal(
                'xdmod-dev-jobs-metrics.csv', valid_dw.get_metrics('Jobs')
            )

    def test_get_dimensions(self, valid_dw):
        with valid_dw:
            self.__assert_dfs_equal(
                'xdmod-dev-jobs-dimensions.csv',
                valid_dw.get_dimensions('Jobs')
            )

    def test_get_filters(self, valid_dw):
        with valid_dw:
            self.__assert_dfs_equal(
                'xdmod-dev-jobs-fieldofscience-filters.csv',
                valid_dw.get_filters('Jobs', 'Field of Science')
            )
