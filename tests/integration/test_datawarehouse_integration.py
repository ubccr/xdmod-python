from dotenv import load_dotenv
import os
from os.path import expanduser
import pandas
from pathlib import Path
import pytest
from xdmod_data.warehouse import DataWarehouse

VALID_XDMOD_URL = os.environ['XDMOD_HOST']
TOKEN_PATH = '~/.xdmod-data-token'
INVALID_STR = 'asdlkfjsdlkfisdjkfjd'
METHOD_PARAMS = {
    'get_data': (
        'duration',
        'realm',
        'metric',
        'dimension',
        'filters',
        'dataset_type',
        'aggregation_unit',
    ),
    'get_raw_data': (
        'duration',
        'realm',
        'filters',
        'fields',
        'show_progress',
    ),
    'describe_realms': (),
    'describe_metrics': ('realm',),
    'describe_dimensions': ('realm',),
    'get_filter_values': ('realm', 'dimension'),
    'describe_raw_realms': (),
    'describe_raw_fields': ('realm',),
}
VALID_DATE = '2016-12-25'
VALID_DIMENSION = 'Resource'
VALID_VALUES = {
  'duration': 'Yesterday',
  'realm': 'Jobs',
  'metric': 'CPU Hours: Total',
  'dimension': VALID_DIMENSION,
  'filters': {VALID_DIMENSION: 'phillips'},
  'dataset_type': 'timeseries',
  'aggregation_unit': 'Auto',
  'parameter': 'duration',
  'fields': ['Nodes'],
  'show_progress': False,
}
KEY_ERROR_TEST_VALUES_AND_MATCHES = {
    'duration': (INVALID_STR, 'Invalid value for `duration`'),
    'realm': (INVALID_STR, r'Realm .* not found'),
    'metric': (INVALID_STR, r'Metric .* not found'),
    'dimension': (INVALID_STR, r'Dimension .* not found'),
    'filter_key': ({INVALID_STR: INVALID_STR}, r'Dimension .* not found'),
    'filter_value': (
        {VALID_DIMENSION: INVALID_STR},
        r'Filter value .* not found',
    ),
    'dataset_type': (INVALID_STR, 'Invalid value for `dataset_type`'),
    'aggregation_unit': (INVALID_STR, 'Invalid value for `aggregation_unit`'),
    'parameter': (
        INVALID_STR,
        'Parameter .* does not have a list of valid values',
    ),
    'field': (INVALID_STR, r'Field .* not found'),
}

key_error_test_ids = []
duration_test_ids = []
start_end_test_ids = []
type_error_test_ids = []

default_valid_params = {}
key_error_test_params = []
date_malformed_test_params = []
type_error_test_params = []
value_error_test_methods = []

for method in METHOD_PARAMS:
    default_valid_params[method] = {}
    for param in METHOD_PARAMS[method]:
        default_valid_params[method][param] = VALID_VALUES[param]
        type_error_test_ids += [method + ':' + param]
        type_error_test_params += [(method, param)]
        if param in KEY_ERROR_TEST_VALUES_AND_MATCHES:
            key_error_test_ids += [method + ':' + param]
            (value, match) = KEY_ERROR_TEST_VALUES_AND_MATCHES[param]
            key_error_test_params += [(method, {param: value}, match)]
        if param == 'duration':
            duration_test_ids += [method]
            start_end_test_ids += [
                method + ':start_date',
                method + ':end_date',
            ]
            date_malformed_test_params += [
                (
                    method,
                    'start_date',
                    {'duration': (INVALID_STR, VALID_DATE)},
                ),
                (
                    method,
                    'end_date',
                    {'duration': (VALID_DATE, INVALID_STR)},
                ),
            ]
            value_error_test_methods += [method]
    if 'filters' in METHOD_PARAMS[method]:
        for param in ('filter_key', 'filter_value'):
            key_error_test_ids += [method + ':' + param]
            (value, match) = KEY_ERROR_TEST_VALUES_AND_MATCHES[param]
            key_error_test_params += [(method, {'filters': value}, match)]


load_dotenv(Path(expanduser(TOKEN_PATH)), override=True)


@pytest.fixture(scope='module')
def dw_methods(request):
    xdmod_host = VALID_XDMOD_URL
    if hasattr(request, 'param'):
        xdmod_host = request.param
    with DataWarehouse(xdmod_host) as dw:
        yield __get_dw_methods(dw)


@pytest.fixture(scope='module')
def dw_methods_outside_runtime_context():
    dw = DataWarehouse(VALID_XDMOD_URL)
    return __get_dw_methods(dw)


def __get_dw_methods(dw):
    return {
        'get_data': dw.get_data,
        'get_raw_data': dw.get_raw_data,
        'describe_realms': dw.describe_realms,
        'describe_metrics': dw.describe_metrics,
        'describe_dimensions': dw.describe_dimensions,
        'get_filter_values': dw.get_filter_values,
        'describe_raw_realms': dw.describe_raw_realms,
        'describe_raw_fields': dw.describe_raw_fields,
    }


def __run_method(dw_methods, method, additional_params={}):
    params = {**default_valid_params[method], **additional_params}
    return dw_methods[method](**params)


def __test_exception(dw_methods, method, additional_params, error, match):
    with pytest.raises(error, match=match):
        __run_method(dw_methods, method, additional_params)


@pytest.mark.parametrize(
    'method, params, match',
    key_error_test_params,
    ids=key_error_test_ids,
)
def test_KeyError(dw_methods, method, params, match):
    __test_exception(dw_methods, method, params, KeyError, match)


@pytest.mark.parametrize(
    'method',
    list(METHOD_PARAMS.keys()),
)
def test_RuntimeError_outside_context(
    dw_methods_outside_runtime_context,
    method,
):
    __test_exception(
        dw_methods_outside_runtime_context,
        method,
        {},
        RuntimeError,
        'outside of the runtime context',
    )


@pytest.mark.parametrize(
    'method, param, params',
    date_malformed_test_params,
    ids=start_end_test_ids,
)
def test_RuntimeError_date_malformed(dw_methods, method, param, params):
    __test_exception(
        dw_methods,
        method,
        params,
        RuntimeError,
        param,
    )


@pytest.mark.parametrize(
    'method, param',
    type_error_test_params,
    ids=type_error_test_ids,
)
def test_TypeError(dw_methods, method, param):
    __test_exception(dw_methods, method, {param: 2}, TypeError, param)


@pytest.mark.parametrize(
    'method',
    value_error_test_methods,
    ids=duration_test_ids,
)
def test_ValueError_duration(dw_methods, method):
    __test_exception(
        dw_methods,
        method,
        {'duration': ('1', '2', '3')},
        ValueError,
        'duration',
    )


def __test_DataFrame_return_value(
    dw_methods,
    method,
    additional_params,
    dtype,
    columns_name,
    columns_data,
    columns_data_subset,
    index_type,
    index_dtype,
    index_name,
    index_size,
):
    df = __run_method(dw_methods, method, additional_params)
    assert isinstance(df, pandas.core.frame.DataFrame)
    for actual_dtype in df.dtypes:
        assert actual_dtype == dtype
    assert isinstance(df.columns, pandas.core.indexes.base.Index)
    assert df.columns.dtype == 'string'
    assert df.columns.name == columns_name
    if columns_data_subset:
        for column in df.columns.to_list():
            assert column in columns_data
    else:
        assert df.columns.to_list() == columns_data
    assert isinstance(df.index, index_type)
    assert df.index.dtype == index_dtype
    assert df.index.name == index_name
    if index_size is not None:
        assert df.index.size == index_size


get_data_return_value_test_params = {
    'duration': ('2020-01-01', '2020-01-31'),
    'realm': 'Jobs',
    'metric': 'Number of Users: Active',
    'dimension': 'None',
    'filters': {},
    'dataset_type': 'timeseries',
    'aggregation_unit': 'Day',
}


@pytest.mark.parametrize(
    'additional_params, columns_name, index_size',
    [
        (
            {},
            'Metric',
            0,
        ),
        (
            {'filters': {'Resource': 'robertson'}},
            'Metric',
            0,
        ),
        (
            {'dimension': 'Resource'},
            'Resource',
            0,
        ),
        (
            {
                'dimension': 'Resource',
                'filters': {'Resource': 'robertson'},
            },
            'Resource',
            0,
        ),
    ],
    ids=(
        'no_dimension,not_empty',
        'no_dimension,empty',
        'dimension,not_empty',
        'dimension,empty',
    ),
)
def test_get_data_timeseries_return_value(
    dw_methods,
    additional_params,
    columns_name,
    index_size,
):
    params = {**get_data_return_value_test_params, **additional_params}
    if columns_name == 'Metric':
        columns_data = [get_data_return_value_test_params['metric']]
        columns_data_subset = False
    else:
        columns_data = dw_methods['get_filter_values'](
            params['realm'],
            params['dimension'],
        )['label'].to_list()
        columns_data_subset = True
    __test_DataFrame_return_value(
        dw_methods,
        method='get_data',
        additional_params=params,
        dtype='Float64',
        columns_name=columns_name,
        columns_data=columns_data,
        columns_data_subset=columns_data_subset,
        index_type=pandas.core.indexes.datetimes.DatetimeIndex,
        index_dtype='datetime64[ns]',
        index_name='Time',
        index_size=index_size,
    )


get_data_aggregate_return_value_test_params = {
    **get_data_return_value_test_params,
    **{'dataset_type': 'aggregate'},
}


@pytest.mark.parametrize(
    'additional_params, index_name, index_size',
    [
        (
            {},
            None,
            1,
        ),
        (
            {'filters': {'Resource': 'robertson'}},
            None,
            1,
        ),
        (
            {'dimension': 'Resource'},
            'Resource',
            0,
        ),
        (
            {
                'dimension': 'Resource',
                'filters': {'Resource': 'robertson'},
            },
            'Resource',
            0,
        ),
    ],
    ids=(
        'no_dimension,not_empty',
        'no_dimension,empty',
        'dimension,not_empty',
        'dimension,empty',
    ),
)
def test_get_data_aggregate_return_value(
    dw_methods,
    additional_params,
    index_name,
    index_size,
):
    params = {
        **get_data_aggregate_return_value_test_params,
        **additional_params,
    }
    series = __run_method(dw_methods, 'get_data', params)
    assert isinstance(series, pandas.core.series.Series)
    assert series.dtype == 'Float64'
    if index_name is None:
        assert series.name is None
        assert series.index.to_list() == [params['metric']]
    else:
        assert series.name == params['metric']
        dimension_values = dw_methods['get_filter_values'](
            params['realm'],
            params['dimension'],
        )['label'].to_list()
        assert series.index.to_list().sort() == dimension_values.sort()
    assert isinstance(series.index, pandas.core.indexes.base.Index)
    assert series.index.dtype == 'string'
    assert series.index.name == index_name
    assert series.index.size == index_size


get_descriptors_return_value_test_columns_data = {
    'describe_realms': ['label'],
    'describe_metrics': ['label', 'description'],
    'describe_dimensions': ['label', 'description'],
    'get_filter_values': ['label'],
    'describe_raw_realms': ['label'],
    'describe_raw_fields': ['label', 'description'],
}
get_descriptors_return_value_test_params = [
    (method, columns_data) for method, columns_data
    in get_descriptors_return_value_test_columns_data.items()
]


@pytest.mark.parametrize(
    'method, columns_data',
    get_descriptors_return_value_test_params,
    ids=get_descriptors_return_value_test_columns_data.keys(),
)
def test_get_descriptors_return_value(dw_methods, method, columns_data):
    __test_DataFrame_return_value(
        dw_methods,
        method,
        additional_params={},
        dtype='string',
        columns_name=None,
        columns_data=columns_data,
        columns_data_subset=False,
        index_type=pandas.core.indexes.base.Index,
        index_dtype='string',
        index_name='id',
        index_size=None,
    )


@pytest.mark.parametrize(
    'method, param, value1, value2',
    [
        ('get_data', 'duration', 'Quarter to date', '  quaRterto dAte '),
        ('get_data', 'aggregation_unit', 'Month', ' m O ntH  '),
    ],
    ids=('get_data:duration', 'get_data:aggregation_unit'),
)
def test_case_insensitive(dw_methods, method, param, value1, value2):
    data1 = __run_method(dw_methods, method, {param: value1})
    data2 = __run_method(dw_methods, method, {param: value2})
    assert data1.equals(data2)


@pytest.mark.parametrize(
    'dw_methods,method',
    [(VALID_XDMOD_URL + '/', method) for method in list(METHOD_PARAMS.keys())],
    indirect=['dw_methods'],
    ids=[method for method in list(METHOD_PARAMS.keys())],
)
def test_trailing_slashes(dw_methods, method):
    __run_method(dw_methods, method)
