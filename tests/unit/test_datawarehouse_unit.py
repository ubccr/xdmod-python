import pytest
import xdmod.datawarehouse as xdw
import os


VALID_XDMOD_URL = 'https://xdmod.access-ci.org'
INVALID_STR = 'asdlkfjsdlkfisdjkfjd'


@pytest.fixture
def tmp_environ_no_user():
    old_environ = dict(os.environ)
    os.environ.clear()
    yield
    os.environ.clear()
    os.environ.update(old_environ)


@pytest.fixture
def tmp_environ_unauth_user(tmp_environ_no_user):
    os.environ['XDMOD_USER'] = INVALID_STR
    os.environ['XDMOD_PASS'] = INVALID_STR


def test___init___KeyError_XDMOD_USER(tmp_environ_no_user):
    with pytest.raises(
            KeyError,
            match='XDMOD_USER environment variable has not been set.'):
        xdw.DataWarehouse(VALID_XDMOD_URL)


def test___init___KeyError_XDMOD_PASS(tmp_environ_no_user):
    os.environ['XDMOD_USER'] = INVALID_STR
    with pytest.raises(
            KeyError,
            match='XDMOD_PASS environment variable has not been set.'):
        xdw.DataWarehouse(VALID_XDMOD_URL)


def test___init___TypeError_xdmod_host(tmp_environ_unauth_user):
    with pytest.raises(TypeError, match='`xdmod_host` must be a string.'):
        xdw.DataWarehouse(2)


def test___init___TypeError_api_token(tmp_environ_unauth_user):
    with pytest.raises(TypeError, match='`api_token` must be a string.'):
        xdw.DataWarehouse('', 2)


def test___enter___RuntimeError_xdmod_host_malformed(tmp_environ_unauth_user):
    with pytest.raises(
            RuntimeError,
            match='Could not connect to xdmod_host \'\': Malformed URL.'):
        with xdw.DataWarehouse(''):
            pass


def test___enter___RuntimeError_xdmod_host_unresolved(tmp_environ_unauth_user):
    invalid_host = INVALID_STR + '.xdmod.org'
    with pytest.raises(
            RuntimeError,
            match='Could not connect to xdmod_host \'' + invalid_host
            + '\': Could not resolve host: ' + invalid_host):
        with xdw.DataWarehouse(invalid_host):
            pass


def test___enter___RuntimeError_xdmod_host_unsupported_protocol(
        tmp_environ_unauth_user):
    invalid_host = INVALID_STR + '://' + INVALID_STR
    with pytest.raises(
            RuntimeError,
            match='Could not connect to xdmod_host \'' + invalid_host
            + '\': Protocol "' + INVALID_STR
            + '" not supported or disabled in libcurl'):
        with xdw.DataWarehouse(invalid_host):
            pass
