import unittest
import os
import xdmod.datawarehouse as xdw

class TestDataWarehouse(unittest.TestCase):
    __XDMOD_URL = 'https://xdmod-dev.ccr.xdmod.org'

    def setUp(self):
        self.__valid_dw = xdw.DataWarehouse(self.__XDMOD_URL)

    def tearDown(self):
        pass

    def test___init___KeyError_XDMOD_USER(self):
        old_environ = dict(os.environ)
        os.environ.clear()
        try:
            with self.assertRaises(KeyError):
                xdw.DataWarehouse(self.__XDMOD_URL)
        finally:
            os.environ.clear()
            os.environ.update(old_environ)

    def test___init___KeyError_XDMOD_PASS(self):
        old_environ = dict(os.environ)
        os.environ.clear()
        os.environ['XDMOD_USER'] = 'sdfjlksdf';
        try:
            with self.assertRaises(KeyError):
                xdw.DataWarehouse(self.__XDMOD_URL)
        finally:
            os.environ.clear()
            os.environ.update(old_environ)

    def test___init___TypeError_xdmod_host(self):
        with self.assertRaises(TypeError):
            xdw.DataWarehouse(2)

    def test___init___TypeError_api_key(self):
        with self.assertRaises(TypeError):
            xdw.DataWarehouse(self.__XDMOD_URL, 2)

    def test___enter___RuntimeError_xdmod_host_malformed(self):
        with self.assertRaises(RuntimeError):
            with xdw.DataWarehouse(''):
                pass

    def test___enter___RuntimeError_xdmod_host_unresolved(self):
        with self.assertRaises(RuntimeError):
            with xdw.DataWarehouse('asdfsdf.xdmod.org'):
                pass

    def test___enter___RuntimeError_xdmod_host_bad_port(self):
        with self.assertRaises(RuntimeError):
            with xdw.DataWarehouse('xdmod-dev.ccr.xdmod.org:0'):
                pass

    def test___enter___RuntimeError_xdmod_host_unsupported_protocol(self):
        with self.assertRaises(RuntimeError):
            with xdw.DataWarehouse('asdklsdfj://sdlkfs'):
                pass

    def test_get_realms_RuntimeError_outside_context(self):
        with self.assertRaises(RuntimeError):
            x = xdw.DataWarehouse(self.__XDMOD_URL)
            self.__valid_dw.get_realms()

    def test_get_dataset_KeyError_duration(self):
        with self.__valid_dw:
            with self.assertRaises(KeyError):
                self.__valid_dw.get_dataset(duration='asdf')

    def test_get_dataset_KeyError_realm(self):
        with self.__valid_dw:
            with self.assertRaises(KeyError):
                self.__valid_dw.get_dataset(realm='asdf')

    def test_get_dataset_KeyError_metric(self):
        with self.__valid_dw:
            with self.assertRaises(KeyError):
                self.__valid_dw.get_dataset(metric='asdf')

    def test_get_dataset_KeyError_dimension(self):
        with self.__valid_dw:
            with self.assertRaises(KeyError):
                self.__valid_dw.get_dataset(dimension='asdf')

    def test_get_dataset_KeyError_filters(self):
        with self.__valid_dw:
            with self.assertRaises(KeyError):
                self.__valid_dw.get_dataset(filters={'asdf'})

    def test_get_dataset_KeyError_dataset_type(self):
        with self.__valid_dw:
            with self.assertRaises(KeyError):
                self.__valid_dw.get_dataset(dataset_type='asdf')

    def test_get_dataset_KeyError_aggregation_unit(self):
        with self.__valid_dw:
            with self.assertRaises(KeyError):
                self.__valid_dw.get_dataset(aggregation_unit='asdf')

    def test_get_dataset_RuntimeError_outside_context(self):
        with self.assertRaises(RuntimeError):
            self.__valid_dw.get_dataset()

    def test_get_dataset_TypeError_duration(self):
        with self.__valid_dw:
            with self.assertRaises(TypeError):
                self.__valid_dw.get_dataset(duration=1)

    def test_get_dataset_TypeError_realm(self):
        with self.__valid_dw:
            with self.assertRaises(TypeError):
                self.__valid_dw.get_dataset(realm=1)

    def test_get_dataset_TypeError_metric(self):
        with self.__valid_dw:
            with self.assertRaises(TypeError):
                self.__valid_dw.get_dataset(metric=1)

    def test_get_dataset_TypeError_dimension(self):
        with self.__valid_dw:
            with self.assertRaises(TypeError):
                self.__valid_dw.get_dataset(dimension=1)

    def test_get_dataset_TypeError_filters(self):
        with self.__valid_dw:
            with self.assertRaises(TypeError):
                self.__valid_dw.get_dataset(filters=1)

    def test_get_dataset_TypeError_dataset_type(self):
        with self.__valid_dw:
            with self.assertRaises(TypeError):
                self.__valid_dw.get_dataset(dataset_type=1)

    def test_get_dataset_TypeError_aggregation_unit(self):
        with self.__valid_dw:
            with self.assertRaises(TypeError):
                self.__valid_dw.get_dataset(aggregation_unit=1)

    def test_get_dataset_ValueError_duration(self):
        with self.__valid_dw:
            with self.assertRaises(ValueError):
                self.__valid_dw.get_dataset(duration=('1', '2', '3'))

    def test_get_dataset_RuntimeError_start_date_malformed(self):
        with self.__valid_dw:
            with self.assertRaises(RuntimeError):
                self.__valid_dw.get_dataset(duration=('asdf', '2022-01-01'))

    def test_get_dataset_RuntimeError_end_date_malformed(self):
        with self.__valid_dw:
            with self.assertRaises(RuntimeError):
                self.__valid_dw.get_dataset(duration=('2022-01-01', 'asdf'))

if __name__ == '__main__':
    unittest.main()
