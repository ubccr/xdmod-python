import numpy as np
import pandas as pd
from xdmod_data._descriptors import _Descriptors
from xdmod_data._http_requester import _HttpRequester
import xdmod_data._response_processor as _response_processor
import xdmod_data._validator as _validator


class DataWarehouse:
    """Access the XDMoD data warehouse via XDMoD's network API.

       Methods must be called within a runtime context using the ``with``
       keyword, e.g.,

       >>> with DataWarehouse('https://xdmod.access-ci.org') as dw:
       ...     dw.get_data()

       Parameters
       ----------
       xdmod_host : str
           The URL of the XDMoD server.

       Raises
       ------
       KeyError
           If the `XDMOD_API_TOKEN` environment variable has not been set.
       RuntimeError
           If a connection cannot be made to the XDMoD server specified by
           `xdmod_host`.
       TypeError
           If `xdmod_host` is not a string.
    """

    def __init__(self, xdmod_host):
        self.__in_runtime_context = False
        self.__http_requester = _HttpRequester(xdmod_host)
        self.__descriptors = _Descriptors(self.__http_requester)

    def __enter__(self):
        self.__in_runtime_context = True
        self.__http_requester._start_up()
        return self

    def __exit__(self, type_, value, traceback):
        self.__http_requester._tear_down()
        self.__in_runtime_context = False

    def get_data(
        self,
        duration='Previous month',
        realm='Jobs',
        metric='CPU Hours: Total',
        dimension='None',
        filters={},
        dataset_type='timeseries',
        aggregation_unit='Auto',
    ):
        """Get a data frame or series containing data from the warehouse.

           If `dataset_type` is 'timeseries', a Pandas DataFrame is returned.
           The data in the DataFrame are the float64 values for the
           corresponding values of time, `metric`, and `dimension`. Missing
           values are filled in with the value `np.nan`. In the DataFrame, the
           index is a DatetimeIndex with the name 'Time' that contains the time
           values for the given `duration` in increments determined by
           `aggregation_unit`. If `dimension` is 'None', the DataFrame columns
           are an index named 'Metric' whose datum is the label of the given
           `metric`. If `dimension` is not 'None', the DataFrame columns are an
           index whose name is the label of the given `dimension` and whose
           data are the labels of each of the values of the given `dimension`.

           If `dataset_type` is 'aggregate', a Pandas Series is returned. The
           data in the Series are the float64 values for the corresponding
           value of `dimension`. Missing values are filled in with the value
           `np.nan`. If `dimension` is 'None', the Series is unnamed, and the
           index is unnamed and contains only the label of the given `metric`.
           If `dimension` is not 'None', the name of the Series is the label of
           the given `metric`, the name of the index is the label of the given
           `dimension`, and the index contains the labels of each of the values
           of the given `dimension`.

           Parameters
           ----------
           duration : str or object of length 2 of str, optional
               The time period over which to collect data. Either a string
               value from `get_durations()` (case insensitive) or an object of
               length two with start and end dates specified in 'YYYY-MM-DD'
               format.
           realm : str, optional
               A realm in the data warehouse. Can be specified by its ID or its
               label. See `describe_realms()`.
           metric : str, optional
               A metric in the given realm of the data warehouse. Can be
               specified by its ID or its label. See `describe_metrics()`.
           dimension : str, optional
               A dimension of the given realm in the data warehouse. Can be
               specified by its ID or its label. See `describe_dimensions()`.
           filters : mapping, optional
               A mapping of dimensions to their possible values. Results will
               only be included whose values for each of the given dimensions
               match one of the corresponding given values.
           dataset_type : str, optional
               Either 'timeseries' or 'aggregate'.
           aggregation_unit : str, optional
               The units by which to aggregate data. Must be one of the valid
               values from `get_aggregation_units()` (case insensitive).

           Returns
           -------
           pandas.core.frame.DataFrame | pandas.core.series.Series

           Raises
           ------
           KeyError
               If any of the parameters have invalid values. Valid realms
               come from `describe_realms()`, valid metrics come from
               `describe_metrics()`, valid dimensions and filter keys come from
               `describe_dimensions()`, valid filter values come from
               `get_filter_values()`, valid durations come from
               `get_durations()`, and aggregation units come from
               `get_aggregation_units()`.
           RuntimeError
               If this method is called outside the runtime context or if
               there is an error requesting data from the warehouse.
           TypeError
               If any of the arguments are of the wrong type.
           ValueError
               If `duration` is an object but not of length 2.
        """
        _validator._assert_runtime_context(self.__in_runtime_context)
        params = _validator._validate_get_data_params(
            self,
            self.__descriptors,
            locals(),
        )
        response = self.__http_requester._request_data(params)
        return _response_processor._process_get_data_response(
            self,
            params,
            response,
        )

    def get_raw_data(
        self,
        duration,
        realm,
        fields=(),
        filters={},
        show_progress=False,
    ):
        """Get a data frame containing raw data from the warehouse.

           Parameters
           ----------
           duration : str or object of length 2 of str
               The time period over which to collect data. Either a string
               value from `get_durations()` (case insensitive) or an object of
               length two with start and end dates specified in 'YYYY-MM-DD'
               format.
           realm : str
               A realm in the data warehouse. Can be specified by its ID or its
               label. See `describe_realms()`.
           fields : sequence of str, optional
               The raw data fields to include in the results. See
               `describe_raw_fields()`.
           filters : mapping, optional
               A mapping of dimensions to their possible values. Results will
               only be included whose values for each of the given dimensions
               match one of the corresponding given values.
           show_progress : bool, optional
               If true, periodically print how many rows have been gotten so
               far.

           Returns
           -------
           pandas.core.frame.DataFrame
               The columns of the data frame are each of the given `fields`.
               The data in the data frame are each of the corresponding values
               for the given `fields`. Missing values are filled with the value
               `numpy.nan`.

           Raises
           ------
           KeyError
               If any of the parameters have invalid values. Valid durations
               come from `get_durations()`, valid realms come from
               `describe_raw_realms()`, valid filters keys come from
               `describe_dimensions()`, valid filter values come from
               `get_filter_values()`, and valid fields come from
               `describe_raw_fields()`.
           RuntimeError
               If this method is called outside the runtime context or if
               there is an error requesting data from the warehouse.
           TypeError
               If any of the arguments are of the wrong type.
           ValueError
               If `duration` is an object but not of length 2.
        """
        _validator._assert_runtime_context(self.__in_runtime_context)
        params = _validator._validate_get_raw_data_params(
            self,
            self.__descriptors,
            locals(),
        )
        (data, column_data) = self.__http_requester._request_raw_data(params)
        return self.__get_data_frame(data, column_data)

    def describe_realms(self):
        """Get a data frame describing the valid realms in the data warehouse.

           Returns
           -------
           pandas.core.frame.DataFrame
               A Pandas DataFrame containing the ID and label of each realm.

           Raises
           ------
           RuntimeError
               If this method is called outside the runtime context.
        """
        _validator._assert_runtime_context(self.__in_runtime_context)
        return self.__get_data_frame_from_descriptor(
            self.__descriptors._get_aggregate(),
            ('id', 'label'),
            'id',
        )

    def describe_metrics(self, realm):
        """Get a data frame describing the valid metrics for the given realm.

           Parameters
           ----------
           realm : str
               A realm in the data warehouse. Can be specified by its ID or its
               label. See `describe_realms()`.

           Returns
           -------
           pandas.core.frame.DataFrame
               A Pandas DataFrame containing the ID, label, and description
               of each metric.

           Raises
           ------
           KeyError
               If `realm` is not one of the values from `describe_realms()`.
           RuntimeError
               If this method is called outside the runtime context.
           TypeError
               If `realm` is not a string.
        """
        return self.__describe_metrics_or_dimensions(realm, 'metrics')

    def describe_dimensions(self, realm):
        """Get a data frame describing the valid dimensions for the given
           realm.

           Parameters
           ----------
           realm : str
               A realm in the data warehouse. Can be specified by its ID or its
               label. See `describe_realms()`.

           Returns
           -------
           pandas.core.frame.DataFrame
               A Pandas DataFrame containing the ID, label, and description
               of each dimension.

           Raises
           ------
           KeyError
               If `realm` is not one of the values from `describe_realms()`.
           RuntimeError
               If this method is called outside the runtime context.
           TypeError
               If `realm` is not a string.
        """
        return self.__describe_metrics_or_dimensions(realm, 'dimensions')

    def get_filter_values(self, realm, dimension):
        """Get a data frame containing the valid filter values for the given
           dimension of the given realm.

           Parameters
           ----------
           realm : str
               A realm in the data warehouse. Can be specified by its ID or its
               label. See `describe_realms()`.
           dimension : str
               A dimension of the given realm in the data warehouse. Can be
               specified by its ID or its label. See `describe_dimensions()`.

           Returns
           -------
           pandas.core.frame.DataFrame
               A Pandas DataFrame containing the ID and label of each filter
               value.

           Raises
           ------
           KeyError
               If `realm` is not one of the values from `describe_realms()` or
               `dimension` is not one of the IDs or labels from
               `describe_dimensions()`.
           RuntimeError
               If this method is called outside the runtime context.
           TypeError
               If `realm` or `dimension` are not strings.
        """
        _validator._assert_runtime_context(self.__in_runtime_context)
        realm_id = _validator._find_realm_id(self.__descriptors, realm)
        dimension_id = _validator._find_dimension_id(
            self.__descriptors,
            realm_id,
            dimension,
        )
        path = '/controllers/metric_explorer.php'
        post_fields = {
            'operation': 'get_dimension',
            'dimension_id': dimension_id,
            'realm': realm_id,
            'limit': 10000,
        }
        response = self.__http_requester._request_json(path, post_fields)
        data = [(datum['id'], datum['name']) for datum in response['data']]
        result = self.__get_data_frame(data, ('id', 'label'), 'id')
        return result

    def get_durations(self):
        """Get the valid values of the `duration` parameter of `get_data()` and
           `get_raw_data()`.

           Returns
           -------
           tuple of str
        """
        return _validator._get_durations()

    def get_aggregation_units(self):
        """Get the valid values for the `aggregation_unit` parameter of
           `get_data()`.

           Returns
           -------
           tuple of str
        """
        return _validator._get_aggregation_units()

    def describe_raw_realms(self):
        """Get a data frame describing the valid raw data realms in the data
           warehouse.

           Returns
           -------
           pandas.core.frame.DataFrame
               A Pandas DataFrame containing the ID and label of each raw data
               realm.

           Raises
           ------
           RuntimeError
               If this method is called outside the runtime context.
        """
        _validator._assert_runtime_context(self.__in_runtime_context)
        return self.__get_data_frame_from_descriptor(
            self.__descriptors._get_raw(),
            ('id', 'label'),
            'id',
        )

    def describe_raw_fields(self, realm):
        """Get a data frame describing the raw data fields for the given realm.

           Parameters
           ----------
           realm : str
               A raw data realm in the data warehouse. Can be specified by its
               ID or its label. See `describe_raw_realms()`.

           Returns
           -------
           pandas.core.frame.DataFrame
               A Pandas DataFrame containing the ID, label, and description
               of each field.

           Raises
           ------
           KeyError
               If `realm` is not one of the values from
               `describe_raw_realms()`.
           RuntimeError
               If this method is called outside the runtime context.
           TypeError
               If `realm` is not a string.
        """
        _validator._assert_runtime_context(self.__in_runtime_context)
        realm_id = _validator._find_raw_realm_id(self.__descriptors, realm)
        return self.__get_data_frame_from_descriptor(
            self.__descriptors._get_raw()[realm_id]['fields'],
            ('id', 'label', 'description'),
            'id',
        )

    def _get_metric_label(self, realm, metric_id):
        d = self.__descriptors._get_aggregate()
        return d[realm]['metrics'][metric_id]['label']

    def _get_dimension_label(self, realm, dimension_id):
        if dimension_id == 'none':
            return None
        d = self.__descriptors._get_aggregate()
        return d[realm]['dimensions'][dimension_id]['label']

    def __get_data_frame(self, data, column_data, index=None):
        result = pd.DataFrame(
            data=data,
            columns=pd.Series(
                data=column_data,
                dtype='string',
            ),
            dtype='string',
        ).fillna(value=np.nan)
        if index:
            result = result.set_index(index)
        return result

    def __get_data_frame_from_descriptor(
        self,
        descriptor,
        columns,
        index=None,
    ):
        data = [
            [id_] + [descriptor[id_][column] for column in columns[1:]]
            for id_ in descriptor
        ]
        return self.__get_data_frame(data, columns, index)

    def __describe_metrics_or_dimensions(self, realm, m_or_d):
        _validator._assert_runtime_context(self.__in_runtime_context)
        realm_id = _validator._find_realm_id(self.__descriptors, realm)
        return self.__get_data_frame_from_descriptor(
            self.__descriptors._get_aggregate()[realm_id][m_or_d],
            ('id', 'label', 'description'),
            'id',
        )
