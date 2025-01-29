from dotenv import dotenv_values
import json
import os
from pathlib import Path
import re
import requests
from urllib.parse import urlencode
import xdmod_data._validator as _validator
from xdmod_data.__version__ import __title__, __version__


class _HttpRequester:
    def __init__(self, xdmod_host):
        self.__in_runtime_context = False
        _validator._assert_str('xdmod_host', xdmod_host)
        xdmod_host = re.sub('/+$', '', xdmod_host)
        self.__xdmod_host = xdmod_host
        self.__api_token = None
        if 'XDMOD_API_TOKEN' in os.environ:
            self.__api_token = os.environ['XDMOD_API_TOKEN']
        self.__headers = {
            'User-Agent': __title__ + ' Python v' + __version__,
        }
        self.__requests_session = None
        self.__raw_data_limit = None

    def _start_up(self):
        self.__in_runtime_context = True
        self.__requests_session = requests.Session()
        self.__assert_connection_to_xdmod_host()

    def _tear_down(self):
        if self.__requests_session is not None:
            self.__requests_session.close()
        self.__in_runtime_context = False

    def _request_data(self, params):
        return self.__request(
            path='/controllers/user_interface.php',
            post_fields=self.__get_data_post_fields(params),
        )

    def _request_raw_data(self, params):
        url_params = self.__get_raw_data_url_params(params)
        # Once XDMoD 10.5 is no longer supported, there will be no need to call
        # __get_raw_data_limit(), and the if/else statement below will not be
        # necessary — only the body of the 'if' branch will be needed.
        limit = self.__get_raw_data_limit()
        data = []
        if limit == 'NA':
            response_iter_lines = self.__request(
                path='/rest/v1/warehouse/raw-data?' + url_params,
                post_fields=None,
                stream=True,
            )
            i = 0
            for line in response_iter_lines:
                line_text = line.decode('utf-8').replace('\x1e', '')
                line_json = json.loads(line_text)
                if i == 0:
                    response = {'fields': line_json}
                else:
                    data.append(line_json)
                    # Only print every 10,000 rows to avoid I/O rate errors.
                    if params['show_progress'] and i % 10000 == 0:
                        self.__print_progress_msg(i, '\r')
                i += 1
            if params['show_progress']:
                self.__print_progress_msg(i, 'DONE\n')
        else:
            num_rows = limit
            offset = 0
            while num_rows == limit:
                response = self._request_json(
                    path='/rest/v1/warehouse/raw-data?' + url_params
                    + '&offset=' + str(offset),
                )
                partial_data = response['data']
                data += partial_data
                if params['show_progress']:
                    self.__print_progress_msg(len(data), '\r')
                num_rows = len(partial_data)
                offset += limit
            if params['show_progress']:
                self.__print_progress_msg(len(data), 'DONE\n')
        return (data, response['fields'])

    def _request_filter_values(self, realm_id, dimension_id):
        limit = 10000
        data = []
        num_rows = limit
        offset = 0
        while num_rows == limit:
            response = self._request_json(
                path='/controllers/metric_explorer.php',
                post_fields={
                    'operation': 'get_dimension',
                    'realm': realm_id,
                    'dimension_id': dimension_id,
                    'start': offset,
                    'limit': limit,
                },
            )
            data += response['data']
            num_rows = len(response['data'])
            offset += limit
        return data

    def _request_json(self, path, post_fields=None):
        response = self.__request(path, post_fields)
        return json.loads(response)

    def __assert_connection_to_xdmod_host(self):
        try:
            self.__request()
        except RuntimeError as e:  # pragma: no cover
            raise RuntimeError(
                "Could not connect to xdmod_host '" + self.__xdmod_host
                + "': " + str(e),
            ) from None

    def __request(self, path='', post_fields=None, stream=False):
        _validator._assert_runtime_context(self.__in_runtime_context)
        url = self.__xdmod_host + path
        token_error_msg = (
            'If running in JupyterHub connected with XDMoD, this is likely an'
            + ' error with the JupyterHub. Otherwise, make sure the'
            + ' `XDMOD_API_TOKEN` environment variable is set before the'
            + ' `DataWarehouse` is constructed; it should be set to a valid'
            + ' API token obtained from the XDMoD web portal.'
        )
        if self.__api_token is not None:
            token = self.__api_token
        else:
            try:
                values = dotenv_values(
                    Path(os.path.expanduser('~/.xdmod-jwt.env')),
                )
                token = values['XDMOD_JWT']
            except KeyError:
                raise KeyError(token_error_msg) from None
        headers = {
            **self.__headers,
            **{
                'Authorization': 'Bearer ' + token,
            },
        }
        if post_fields:
            response = self.__requests_session.post(
                url,
                headers=headers,
                data=post_fields,
            )
        else:
            response = self.__requests_session.get(url, headers=headers)
        if response.status_code != 200:
            msg = ''
            try:
                response_json = json.loads(response.text)
                msg = ': ' + response_json['message']
            except json.JSONDecodeError:  # pragma: no cover
                pass
            if response.status_code == 401:
                msg = (
                    ': ' + token_error_msg
                )
            raise RuntimeError(
                'Error ' + str(response.status_code) + msg,
            ) from None
        if stream:
            return response.iter_lines()
        else:
            return response.text

    def __get_data_post_fields(self, params):
        post_fields = {
            'operation': 'get_data',
            'start_date': params['start_date'],
            'end_date': params['end_date'],
            'realm': params['realm'],
            'statistic': params['metric'],
            'group_by': params['dimension'],
            'dataset_type': params['dataset_type'],
            'aggregation_unit': params['aggregation_unit'],
            'format': 'csv',
        }
        for dimension in params['filters']:
            post_fields[dimension + '_filter'] = ','.join(
                params['filters'][dimension],
            )
        return post_fields

    def __get_raw_data_url_params(self, params):
        results = {
            'realm': params['realm'],
            'start_date': params['start_date'],
            'end_date': params['end_date'],
        }
        if (params['fields']):
            results['fields'] = ','.join(params['fields'])
        if (params['filters']):
            for dimension in params['filters']:
                results['filters[' + dimension + ']'] = ','.join(
                    params['filters'][dimension],
                )
        return urlencode(results)

    # Once XDMoD 10.5 is no longer supported,
    # there will be no need for this method.
    def __get_raw_data_limit(self):
        if self.__raw_data_limit is None:
            try:
                response = self._request_json(
                    '/rest/v1/warehouse/raw-data/limit',
                )
                self.__raw_data_limit = int(response['data'])
            except RuntimeError as e:
                if '404' in str(e):
                    self.__raw_data_limit = 'NA'
                else:  # pragma: no cover
                    raise
        return self.__raw_data_limit

    def __print_progress_msg(self, num_rows, end='\n'):
        progress_msg = (
            'Got ' + str(num_rows) + ' row' + ('' if num_rows == 1 else 's')
            + '...'
        )
        print(progress_msg, end=end)
