import json
import os
import requests
from urllib.parse import urlencode
import xdmod_data._validator as _validator
from xdmod_data.__version__ import __title__, __version__


class _HttpRequester:
    def __init__(self, xdmod_host):
        self.__in_runtime_context = False
        _validator._assert_str('xdmod_host', xdmod_host)
        self.__xdmod_host = xdmod_host
        try:
            self.__api_token = os.environ['XDMOD_API_TOKEN']
        except KeyError:
            raise KeyError(
                '`XDMOD_API_TOKEN` environment variable has not been set.'
            ) from None
        self.__headers = {
            'Authorization': 'Bearer ' + self.__api_token,
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
        limit = self.__get_raw_data_limit()
        data = []
        num_rows = limit
        offset = 0
        while num_rows == limit:
            response = self._request_json(
                path='/rest/v1/warehouse/raw-data?' + url_params
                + '&offset=' + str(offset)
            )
            partial_data = response['data']
            data += partial_data
            if params['show_progress']:
                progress_msg = 'Got ' + str(len(data)) + ' rows...'
                print(progress_msg, end='\r')
            num_rows = len(partial_data)
            offset += limit
        if params['show_progress']:
            print(progress_msg + 'DONE')
        return (data, response['fields'])

    def _request_json(self, path, post_fields=None):
        response = self.__request(path, post_fields)
        return json.loads(response)

    def __assert_connection_to_xdmod_host(self):
        try:
            self.__request()
        except RuntimeError as e:
            raise RuntimeError(
                'Could not connect to xdmod_host \'' + self.__xdmod_host
                + '\': ' + str(e)
            ) from None

    def __request(self, path='', post_fields=None):
        _validator._assert_runtime_context(self.__in_runtime_context)
        url = self.__xdmod_host + path
        if post_fields:
            post_fields['Bearer'] = self.__api_token
            response = self.__requests_session.post(
                url,
                headers=self.__headers,
                data=post_fields,
            )
        else:
            url += '&' if '?' in url else '?'
            url += 'Bearer=' + self.__api_token
            response = self.__requests_session.get(url, headers=self.__headers)
        if response.status_code != 200:
            msg = ''
            try:
                response_json = json.loads(response.text)
                msg = ': ' + response_json['message']
            except json.JSONDecodeError:
                pass
            if response.status_code == 401:
                msg = (
                    ': Make sure XDMOD_API_TOKEN is set to a valid API token.'
                )
            raise RuntimeError(
                'Error ' + str(response.status_code) + msg
            ) from None
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
                params['filters'][dimension]
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
                    params['filters'][dimension]
                )
        return urlencode(results)

    def __get_raw_data_limit(self):
        if self.__raw_data_limit is None:
            response = self._request_json('/rest/v1/warehouse/raw-data/limit')
            self.__raw_data_limit = int(response['data'])
        return self.__raw_data_limit
