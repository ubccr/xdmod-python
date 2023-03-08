import csv
from datetime import datetime
import html
import numpy as np
import pandas as pd
import re
from xdmod._descriptors import _Descriptors
from xdmod._http_requester import _HttpRequester
import xdmod._validator as _validator


class DataWarehouse:
    """Access the XDMoD data warehouse via XDMoD's network API.

       Methods must be called within a runtime context using the ``with``
       keyword, e.g.,

       >>> with DataWarehouse(XDMOD_URL, XDMOD_API_TOKEN) as dw:
       ...     dw.get_data()

       Parameters
       ----------
       xdmod_host : str
           The URL of the XDMoD server.
       api_token : str, optional
           The API token used to connect. If not provided, the
           `XDMOD_USER` and `XDMOD_PASS` environment variables must be
           set.

       Raises
       ------
       KeyError
           If `api_token` is None and either or both of the environment
           variables `XDMOD_USER` and `XDMOD_PASS` have not been set.
       RuntimeError
           If a connection cannot be made to the XDMoD server specified by
           `xdmod_host`.
       TypeError
           If `xdmod_host` is not a string or if `api_token` is not None and is
           not a string.
    """

    def __init__(self, xdmod_host, api_token=None):
        self.__in_runtime_context = False
        self.__http_requester = _HttpRequester(xdmod_host, api_token)
        self.__descriptors = _Descriptors(self.__http_requester)
        self.__username = None

    def __enter__(self):
        self.__in_runtime_context = True
        self.__http_requester._start_up()
        return self

    def __exit__(self, type_, value, traceback):
        self.__http_requester._tear_down()
        self.__username = None
        self.__in_runtime_context = False

    def get_data(
        self, duration='Previous month', realm='Jobs',
        metric='CPU Hours: Total', dimension='None', filters={},
        timeseries=True, aggregation_unit='Auto'
    ):
        """Get a data frame or series containing data from the warehouse.

           If `timeseries` is True, a Pandas DataFrame is returned. In that
           DataFrame, the index has the name 'Time' and contains
           the time values for the given `duration` in increments
           determined by `aggregation_unit`. The columns of the DataFrame are
           a Pandas Series that has the same properties as the Series that is
           returned if `timeseries` were instead False (see paragraph below).
           The data in the DataFrame are the float64 values for the
           corresponding values of time, `metric`, and `dimension`.

           If `timeseries` is False, a Pandas Series is returned. The name of
           the Series is the value of `metric`. The index of the Series
           has a name equal to `dimension`, and the index has the
           corresponding values for that `dimension` (as can be obtained via
           `get_filters()`). If `dimension` is None, the Series has a name
           equal to the organization name configured by the instance of XDMoD
           whose URL is passed into the `DataWarehouse()` constructor as
           `xdmod_host`. The data in the series are the float64 values for the
           corresponding values of `metric` and `dimension`.

           Parameters
           ----------
           duration : str or object of length 2 of str, optional
               The time period over which to collect data. Either a string
               value from `get_durations()` (case insensitive) or an object of
               length two with start and end dates specified in 'YYYY-MM-DD'
               format.
           realm : str, optional
               A realm in the data warehouse. Can be specified by its ID or its
               label. See `get_realms()`.
           metric : str, optional
               A metric in the given realm of the data warehouse. Can be
               specified by its ID or its label. See `get_metrics()`.
           dimension : str, optional
               A dimension of the given realm in the data warehouse. Can be
               specified by its ID or its label. See `get_dimensions()`.
           filters : mapping, optional
               A mapping of dimensions to their possible values. Results will
               only be included whose values for each of the given dimensions
               match one of the corresponding given values.
           timeseries : bool, optional
               Whether to return timeseries data (True) or aggregate data
               (False).
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
               come from `get_realms()`, valid metrics come from
               `get_metrics()`, valid dimensions and filter keys come from
               `get_dimensions()`, valid filter values come from
               `get_filters()`, valid durations come from
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
            self, self.__descriptors, locals()
        )
        response = self.__http_requester._request_data(params)
        return self.__process_get_data_response(params, response)

    def __process_get_data_response(self, params, response):
        csvdata = csv.reader(response.splitlines())
        if not params['timeseries']:
            return self.__xdmod_csv_to_pandas(csvdata)
        else:
            labelre = re.compile(r'\[([^\]]+)\].*')
            timestamps = []
            data = []
            for line_num, line in enumerate(csvdata):
                if line_num == 5:
                    start_date, end_date = line
                elif line_num == 7:
                    dimensions = []
                    for label in line[1:]:
                        match = labelre.match(label)
                        if match:
                            dimensions.append(html.unescape(match.group(1)))
                        else:
                            dimensions.append(html.unescape(label))
                elif line_num > 7 and len(line) > 1:
                    date_string = line[0]
                    # Match YYYY-MM-DD
                    if re.match(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$', line[0]):
                        format = '%Y-%m-%d'
                    # Match YYYY-MM
                    elif re.match(r'^[0-9]{4}-[0-9]{2}$', line[0]):
                        format = '%Y-%m'
                    # Match YYYY
                    elif re.match(r'^[0-9]{4}$', line[0]):
                        format = '%Y'
                    # Match YYYY Q#
                    elif re.match(r'^[0-9]{4} Q[0-9]$', line[0]):
                        year, quarter = line[0].split(' ')
                        if quarter == 'Q1':
                            month = '01'
                        elif quarter == 'Q2':
                            month = '04'
                        elif quarter == 'Q3':
                            month = '07'
                        elif quarter == 'Q4':
                            month = '10'
                        else:
                            raise Exception(
                                'Unsupported date quarter specification '
                                + line[0] + '.'
                            )
                        date_string = year + '-' + month + '-01'
                        format = '%Y-%m-%d'
                    else:
                        raise Exception(
                            'Unsupported date specification ' + line[0] + '.'
                        )
                    timestamps.append(datetime.strptime(date_string, format))
                    data.append(np.asarray(line[1:], dtype=np.float64))
            return pd.DataFrame(
                data=data,
                index=pd.Series(data=timestamps, name='Time'),
                columns=pd.Series(
                    dimensions,
                    name=self.__get_dimension_label(
                        params['realm'], params['dimension']
                    )
                )
            )

    def get_raw_data(
        self, duration, realm, fields=(), filters={}, show_progress=False
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
               label. See `get_realms()`.
           fields : sequence of str, optional
               The raw data fields to include in the results. See
               `get_raw_fields()`.
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
               `get_raw_realms()`, valid filters keys come from
               `get_dimensions()`, valid filter values come from
               `get_filters()`, and valid fields come from `get_raw_fields()`.
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
            self, self.__descriptors, locals()
        )
        (data, columns) = self.__http_requester._request_raw_data(params)
        return pd.DataFrame(data=data, columns=columns).fillna(value=np.nan)

    def get_realms(self):
        """Get a data frame containing the valid realms in the data warehouse.

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
        return self.__get_indexed_data_frame_from_descriptor(
            self.__descriptors._get_aggregate(), ('id', 'label')
        )

    def get_metrics(self, realm):
        """Get a data frame containing the valid metrics for the given realm.

           Parameters
           ----------
           realm : str
               A realm in the data warehouse. Can be specified by its ID or its
               label. See `get_realms()`.

           Returns
           -------
           pandas.core.frame.DataFrame
               A Pandas DataFrame containing the ID, label, and description
               of each metric.

           Raises
           ------
           KeyError
               If `realm` is not one of the values from `get_realms()`.
           RuntimeError
               If this method is called outside the runtime context.
           TypeError
               If `realm` is not a string.
        """
        return self.__get_metrics_or_dimensions(realm, 'metrics')

    def get_dimensions(self, realm):
        """Get a data frame containing the valid dimensions for the given
           realm.

           Parameters
           ----------
           realm : str
               A realm in the data warehouse. Can be specified by its ID or its
               label. See `get_realms()`.

           Returns
           -------
           pandas.core.frame.DataFrame
               A Pandas DataFrame containing the ID, label, and description
               of each dimension.

           Raises
           ------
           KeyError
               If `realm` is not one of the values from `get_realms()`.
           RuntimeError
               If this method is called outside the runtime context.
           TypeError
               If `realm` is not a string.
        """
        return self.__get_metrics_or_dimensions(realm, 'dimensions')

    def get_filters(self, realm, dimension):
        """Get a data frame containing the valid filters for the given
           dimension of the given realm.

           Parameters
           ----------
           realm : str
               A realm in the data warehouse. Can be specified by its ID or its
               label. See `get_realms()`.
           dimension : str
               A dimension of the given realm in the data warehouse. Can be
               specified by its ID or its label. See `get_dimensions()`.

           Returns
           -------
           pandas.core.frame.DataFrame
               A Pandas DataFrame containing the ID and label of each filter.

           Raises
           ------
           KeyError
               If `realm` is not one of the values from `get_realms()` or
               `dimension` is not one of the IDs or labels from
               `get_dimensions()`.
           RuntimeError
               If this method is called outside the runtime context.
           TypeError
               If `realm` or `dimension` are not strings.
        """
        _validator._assert_runtime_context(self.__in_runtime_context)
        realm_id = _validator._find_realm_id(self.__descriptors, realm)
        dimension_id = _validator._find_dimension_id(
            self.__descriptors, realm_id, dimension
        )
        path = '/controllers/metric_explorer.php'
        post_fields = {
            'operation': 'get_dimension',
            'dimension_id': dimension_id,
            'realm': realm_id,
            'limit': 10000
        }
        response = self.__http_requester._request_json(path, post_fields)
        data = [(datum['id'], datum['name']) for datum in response['data']]
        result = pd.DataFrame(data=data, columns=('id', 'label'))
        result = result.set_index('id')
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

    def get_raw_realms(self):
        """Get a data frame containing the valid raw data realms in the data
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
        return self.__get_indexed_data_frame_from_descriptor(
            self.__descriptors._get_raw(), ('id', 'label')
        )

    def get_raw_fields(self, realm):
        """Get a data frame containing the raw data fields for the given realm.

           Parameters
           ----------
           realm : str
               A raw data realm in the data warehouse. Can be specified by its
               ID or its label. See `get_raw_realms()`.

           Returns
           -------
           pandas.core.frame.DataFrame
               A Pandas DataFrame containing the ID, label, and description
               of each field.

           Raises
           ------
           KeyError
               If `realm` is not one of the values from `get_raw_realms()`.
           RuntimeError
               If this method is called outside the runtime context.
           TypeError
               If `realm` is not a string.
        """
        _validator._assert_runtime_context(self.__in_runtime_context)
        realm_id = _validator._find_raw_realm_id(self.__descriptors, realm)
        return self.__get_indexed_data_frame_from_descriptor(
            self.__descriptors._get_raw()[realm_id]['fields'],
            ('id', 'label', 'description')
        )

    def __xdmod_csv_to_pandas(self, rd):
        groups = []
        data = []
        for line_num, line in enumerate(rd):
            if line_num == 5:
                start_date, end_date = line
            elif line_num == 7:
                dimension, metric = line
            elif line_num > 7 and len(line) > 1:
                groups.append(html.unescape(line[0]))
                data.append(np.float64(line[1]))
        if len(data) == 0:
            return pd.Series(dtype=np.float64)
        return pd.Series(
            data=data,
            name=metric,
            index=pd.Series(data=groups, name=dimension),
            dtype=np.float64
        )

    def __get_dimension_label(self, realm, dimension_id):
        if dimension_id == 'none':
            return None
        d = self.__descriptors._get_aggregate()
        return d[realm]['dimensions'][dimension_id]['label']

    def __get_indexed_data_frame_from_descriptor(self, descriptor, columns):
        data = [
            [id_] + [descriptor[id_][column] for column in columns[1:]]
            for id_ in descriptor
        ]
        result = pd.DataFrame(data=data, columns=columns)
        result = result.set_index('id')
        return result

    def __get_metrics_or_dimensions(self, realm, m_or_d):
        _validator._assert_runtime_context(self.__in_runtime_context)
        realm_id = _validator._find_realm_id(self.__descriptors, realm)
        return self.__get_indexed_data_frame_from_descriptor(
            self.__descriptors._get_aggregate()[realm_id][m_or_d],
            ('id', 'label', 'description')
        )

    def whoami(self):
        if self.__username:
            return self.__username
        return 'Not logged in'

    def compliance(self, timeframe):
        response = self.__http_requester._request_json(
            '/controllers/compliance.php',
            {'timeframe_mode': timeframe}
        )
        return response

    def resources(self):
        names = []
        types = []
        resource_ids = []
        cdata = self.compliance('to_date')
        for resource in cdata['metaData']['fields']:
            if resource['name'] == 'requirement':
                continue
            names.append(
                resource['header'][:-7].split('>')[1].replace('-', ' ')
            )
            types.append(resource['status'].split('|')[0].strip())
            resource_ids.append(resource['resource_id'])
        return pd.Series(data=types, index=names)

    def get_qualitydata(self, params, is_numpy=False):
        type_to_title = {
            'gpu': '% of jobs with GPU information',
            'hardware': '% of jobs with hardware perf information',
            'cpu': '% of jobs with cpu usage information',
            'script': '% of jobs with Job Batch Script information',
            'realms': '% of jobs in the SUPReMM realm compared to Jobs realm'
        }
        response = self.__http_requester._request_json(
            '/rest/supremm_dataflow/quality', params
        )
        if response['success']:
            result = response['result']
            jobs = [job for job in result]
            dates = [
                date.strftime('%Y-%m-%d') for date in pd.date_range(
                    params['start'], params['end'], freq='D'
                ).date
            ]
            quality = np.empty((len(jobs), len(dates)))
            for i in range(len(jobs)):
                for j in range(len(dates)):
                    job_i = result[jobs[i]]
                    if job_i.get(dates[j], np.nan) != 'N/A':
                        quality[i, j] = job_i.get(dates[j], np.nan)
                    else:
                        quality[i, j] = np.nan
            if is_numpy:
                return quality
            df = pd.DataFrame(data=quality, index=jobs, columns=dates)
            df.name = type_to_title[params['type']]
            return df
