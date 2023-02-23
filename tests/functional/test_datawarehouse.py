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

    def test_get_realms(self, valid_dw):
        with valid_dw:
            expected = pandas.read_csv(
                self.__DATA_DIR + '/xdmod-dev-realms.csv')
            expected = expected.set_index('id')
            actual = valid_dw.get_realms()
            assert expected.equals(actual)

    def test_get_metrics(self, valid_dw):
        with valid_dw:
            expected = pandas.read_csv(
                self.__DATA_DIR + '/xdmod-dev-jobs-metrics.csv')
            expected = expected.set_index('id')
            actual = valid_dw.get_metrics('Jobs')
            assert expected.equals(actual)
