import os
import pytest
import xdmod.datawarehouse as xdw


class TestDataWarehouse:
    __INVALID_STR = 'asdlkfjsdlkfisdjkfjd'
    __VALID_XDMOD_URL = 'https://xdmod.access-ci.org'

    @pytest.fixture
    def tmp_environ_no_user(self):
        old_environ = dict(os.environ)
        os.environ.clear()
        yield
        os.environ.clear()
        os.environ.update(old_environ)

    @pytest.fixture
    def tmp_environ_unauth_user(self, tmp_environ_no_user):
        os.environ['XDMOD_USER'] = self.__INVALID_STR
        os.environ['XDMOD_PASS'] = self.__INVALID_STR

    def test___init___KeyError_XDMOD_USER(self, tmp_environ_no_user):
        with pytest.raises(KeyError, match='XDMOD_USER'):
            xdw.DataWarehouse(self.__VALID_XDMOD_URL)

    def test___init___KeyError_XDMOD_PASS(self, tmp_environ_no_user):
        os.environ['XDMOD_USER'] = self.__INVALID_STR
        with pytest.raises(KeyError, match='XDMOD_PASS'):
            xdw.DataWarehouse(self.__VALID_XDMOD_URL)

    def test___init___TypeError_xdmod_host(self, tmp_environ_unauth_user):
        with pytest.raises(TypeError, match='xdmod_host'):
            xdw.DataWarehouse(2)

    def test___init___TypeError_api_token(self, tmp_environ_unauth_user):
        with pytest.raises(TypeError, match='api_token'):
            xdw.DataWarehouse('', 2)

    def test___enter___RuntimeError_xdmod_host_malformed(self, tmp_environ_unauth_user):
        with pytest.raises(RuntimeError, match='xdmod_host'):
            with xdw.DataWarehouse(''):
                pass

    def test___enter___RuntimeError_xdmod_host_unresolved(self, tmp_environ_unauth_user):
        with pytest.raises(RuntimeError, match='xdmod_host'):
            with xdw.DataWarehouse('asdfsdf.xdmod.org'):
                pass

    def test___enter___RuntimeError_xdmod_host_unsupported_protocol(self, tmp_environ_unauth_user):
        with pytest.raises(RuntimeError, match='xdmod_host'):
            with xdw.DataWarehouse('asdklsdfj://sdlkfs'):
                pass
