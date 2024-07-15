import pytest
import os
import requests
from xdmod_data.warehouse import DataWarehouse


VALID_XDMOD_HOST = os.environ['XDMOD_HOST']
INVALID_STR = 'asdlkfjsdlkfisdjkfjd'


@pytest.fixture(scope='module', autouse=True)
def set_environ():
    token = (
        os.environ['XDMOD_API_TOKEN']
        if 'XDMOD_API_TOKEN' in os.environ
        else ''
    )
    os.environ['XDMOD_API_TOKEN'] = INVALID_STR
    yield
    os.environ['XDMOD_API_TOKEN'] = token


def test___init___TypeError_xdmod_host():
    with pytest.raises(TypeError, match='`xdmod_host` must be a string.'):
        DataWarehouse(2)


def test___init___KeyError():
    token = os.environ['XDMOD_API_TOKEN']
    del os.environ['XDMOD_API_TOKEN']
    with pytest.raises(
        KeyError,
        match='`XDMOD_API_TOKEN` environment variable has not been set.',
    ):
        DataWarehouse(VALID_XDMOD_HOST)
    os.environ['XDMOD_API_TOKEN'] = token


def test___enter___RuntimeError_xdmod_host_malformed():
    with pytest.raises(
        (
            requests.exceptions.InvalidURL,
            requests.exceptions.MissingSchema,
        ),
        match=(
            r'(Invalid URL \'.*\': No host supplied|'
            + r'Invalid URL \'https:\?Bearer=' + INVALID_STR + "': "
            + r'No schema supplied. Perhaps you meant http://https:\?Bearer='
            + INVALID_STR + r'\?)'
        ),
    ):
        with DataWarehouse('https://'):
            pass


def test___enter___RuntimeError_xdmod_host_unresolved():
    invalid_host = 'https://' + INVALID_STR + '.xdmod.org'
    with pytest.raises(Exception):
        with DataWarehouse(invalid_host):
            pass


def test___enter___RuntimeError_xdmod_host_unsupported_protocol():
    invalid_host = INVALID_STR + '://' + INVALID_STR
    with pytest.raises(
        requests.exceptions.InvalidSchema,
        match="No connection adapters were found for '" + invalid_host,
    ):
        with DataWarehouse(invalid_host):
            pass


def test___enter___RuntimeError_401():
    with pytest.raises(
        RuntimeError,
        match='Error 401: Make sure XDMOD_API_TOKEN is set'
        + ' to a valid API token.',
    ):
        with DataWarehouse(VALID_XDMOD_HOST) as dw:
            dw.describe_realms()


def test_exit_without_enter():
    dw = DataWarehouse(VALID_XDMOD_HOST)
    dw.__exit__(None, None, None)
