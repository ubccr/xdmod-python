import pytest
import xdmod.datawarehouse as xdw
import pandas

INVALID_STR = 'asdlkfjsdlkfisdjkfjd'
VALID_XDMOD_URL = 'https://xdmod-dev.ccr.xdmod.org'
METHOD_PARAMS = {
    'get_data': (
        'duration', 'realm', 'metric', 'dimension', 'filters', 'timeseries',
        'aggregation_unit',
    ),
    'get_raw_data': ('duration', 'realm', 'filters', 'fields'),
    'get_realms': (),
    'get_metrics': ('realm',),
    'get_dimensions': ('realm',),
    'get_filters': ('realm', 'dimension',),
    'get_valid_values': ('parameter',),
    'get_raw_realms': (),
    'get_raw_fields': ('realm',),
}
VALID_DATE = '2020-01-01'
VALID_DIMENSION = 'Resource'
VALID_VALUES = {
  'duration': 'Previous month',
  'realm': 'Jobs',
  'metric': 'CPU Hours: Total',
  'dimension': VALID_DIMENSION,
  'filters': {VALID_DIMENSION: 'Expanse'},
  'timeseries': True,
  'aggregation_unit': 'Auto',
  'parameter': 'duration',
  'fields': ['Nodes'],
}
KEY_ERROR_TEST_VALUES_AND_MATCHES = {
    'duration': (INVALID_STR, 'Invalid value for `duration`'),
    'realm': (INVALID_STR, r'Realm .* not found'),
    'metric': (INVALID_STR, r'Metric .* not found'),
    'dimension': (INVALID_STR, r'Dimension .* not found'),
    'filter_key': ({INVALID_STR: INVALID_STR}, r'Dimension .* not found'),
    'filter_value': (
        {VALID_DIMENSION: INVALID_STR}, r'Filter value .* not found'
    ),
    'aggregation_unit': (INVALID_STR, 'Invalid value for `aggregation_unit`'),
    'parameter': (
        INVALID_STR, 'Parameter .* does not have a list of valid values'
    ),
    'field': (INVALID_STR, r'Field .* not found'),
}

key_error_test_names = []
duration_test_names = []
start_end_test_names = []
type_error_test_names = []

default_valid_params = {}
key_error_test_params = []
date_malformed_test_params = []
type_error_test_params = []
value_error_test_methods = []

for method in METHOD_PARAMS:
    default_valid_params[method] = {}
    for param in METHOD_PARAMS[method]:
        default_valid_params[method][param] = VALID_VALUES[param]
        type_error_test_names += [method + ':' + param]
        type_error_test_params += [(method, param)]
        if param in KEY_ERROR_TEST_VALUES_AND_MATCHES:
            key_error_test_names += [method + ':' + param]
            (value, match) = KEY_ERROR_TEST_VALUES_AND_MATCHES[param]
            key_error_test_params += [(method, {param: value}, match)]
        if param == 'duration':
            duration_test_names += [method + ':duration']
            start_end_test_names += [
                method + ':start_date', method + ':end_date'
            ]
            date_malformed_test_params += [
                (method, 'start_date', {'duration': (INVALID_STR, VALID_DATE)}),
                (method, 'end_date', {'duration': (VALID_DATE, INVALID_STR)}),
            ]
            value_error_test_methods += [method]


@pytest.fixture(scope='module')
def dw_methods():
    with xdw.DataWarehouse(VALID_XDMOD_URL) as dw:
        yield __get_dw_methods(dw)


@pytest.fixture(scope='module')
def dw_methods_outside_runtime_context():
    dw = xdw.DataWarehouse(VALID_XDMOD_URL)
    return __get_dw_methods(dw)


def __get_dw_methods(dw):
    return {
        'get_data': dw.get_data,
        'get_raw_data': dw.get_raw_data,
        'get_realms': dw.get_realms,
        'get_metrics': dw.get_metrics,
        'get_dimensions': dw.get_dimensions,
        'get_filters': dw.get_filters,
        'get_valid_values': dw.get_valid_values,
        'get_raw_realms': dw.get_raw_realms,
        'get_raw_fields': dw.get_raw_fields,
    }


def __run_method(dw, method, additional_params={}):
    params = {**default_valid_params[method], **additional_params}
    return dw[method](**params)


def __test_exception(dw_methods, method, additional_params, error, match):
    with pytest.raises(error, match=match):
        __run_method(dw_methods, method, additional_params)


@pytest.mark.parametrize(
    'method, params, match', key_error_test_params, ids=key_error_test_names
)
def test_KeyError(dw_methods, method, params, match):
    __test_exception(dw_methods, method, params, KeyError, match)


@pytest.mark.parametrize(
    'method',
    [
        'get_data',
        'get_raw_data',
        'get_realms',
        'get_metrics',
        'get_dimensions',
        'get_filters',
        'get_raw_realms',
        'get_raw_fields',
    ]
)
def test_RuntimeError_outside_context(
        dw_methods_outside_runtime_context, method):
    __test_exception(
        dw_methods_outside_runtime_context, method, {}, RuntimeError,
        'outside of the runtime context'
    )


@pytest.mark.parametrize(
    'method, param, params',
    date_malformed_test_params,
    ids=start_end_test_names
)
def test_RuntimeError_date_malformed(dw_methods, method, param, params):
    __test_exception(
        dw_methods, method, params, RuntimeError,
        param + ' param is not in the correct format'
    )


@pytest.mark.parametrize(
    'method, param',
    type_error_test_params,
    ids=type_error_test_names
)
def test_TypeError(dw_methods, method, param):
    __test_exception(dw_methods, method, {param: 2}, TypeError, param)


@pytest.mark.parametrize(
    'method',
    value_error_test_methods,
    ids=duration_test_names
)
def test_ValueError_duration(dw_methods, method):
    __test_exception(
        dw_methods, method, {'duration': ('1', '2', '3')}, ValueError,
        'duration'
    )


@pytest.mark.parametrize(
    'method',
    [
        'get_data',
        'get_raw_data',
        'get_realms',
        'get_metrics',
        'get_dimensions',
        'get_filters',
        'get_raw_realms',
        'get_raw_fields',
    ]
)
def test_DataFrame_return_type(dw_methods, method):
    assert isinstance(
        __run_method(dw_methods, method), pandas.core.frame.DataFrame
    )
