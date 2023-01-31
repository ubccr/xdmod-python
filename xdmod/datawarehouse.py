import csv
from datetime import datetime
import html
import io
import json
import numpy
import os
import pandas as pd
import pycurl
import re
import tempfile
from urllib.parse import urlencode


class DataWareHouse:
    """ Access the XDMoD datawarehouse via XDMoD's network API """

    def __init__(self, xdmodhost, apikey=None, sslverify=True):
        self.xdmodhost = xdmodhost
        self.apikey = apikey
        self.logged_in = None
        self.crl = None
        self.cookiefile = None
        self.descriptor = None
        self.sslverify = sslverify
        self.headers = []

        if not self.apikey:
            try:
                self.apikey = {
                    'username': os.environ['XDMOD_USER'],
                    'password': os.environ['XDMOD_PASS']
                }
            except KeyError:
                pass

    def __enter__(self):
        self.crl = pycurl.Curl()

        if not self.sslverify:
            self.crl.setopt(pycurl.SSL_VERIFYPEER, 0)
            self.crl.setopt(pycurl.SSL_VERIFYHOST, 0)

        if self.apikey:
            _, self.cookiefile = tempfile.mkstemp()
            self.crl.setopt(pycurl.COOKIEJAR, self.cookiefile)
            self.crl.setopt(pycurl.COOKIEFILE, self.cookiefile)

            response = self.__request_json('/rest/auth/login',
                                           self.apikey)

            if response['success'] is True:
                token = response['results']['token']
                self.headers = ['Token: ' + token]
                self.crl.setopt(pycurl.HTTPHEADER, self.headers)
                self.logged_in = response['results']['name']
            else:
                raise RuntimeError('Access Denied')

        return self

    def __request_json(self, path, config, headers=None, contentType=None):
        response = self.__request(path, config, headers, contentType)
        return json.loads(response)

    def __request(self, path, config, headers=None, contentType=None):
        if headers is None:
            headers = self.headers
        if contentType == 'JSON':
            pf = config
        else:
            pf = urlencode(config)
        b_obj = io.BytesIO()
        self.crl.reset()
        self.crl.setopt(pycurl.URL, self.xdmodhost + path)
        self.crl.setopt(pycurl.HTTPHEADER, headers)
        self.crl.setopt(pycurl.WRITEDATA, b_obj)
        self.crl.setopt(pycurl.POSTFIELDS, pf)
        self.crl.perform()
        body_bytes = b_obj.getvalue()
        code = self.crl.getinfo(pycurl.RESPONSE_CODE)
        body_text = body_bytes.decode('utf8')
        if code != 200:
            body_json = json.loads(body_text)
            raise RuntimeError('Error ' + str(code) + ': \'' +
                               body_json['message'] + '\'')
        response = body_text
        return response

    def __exit__(self, tpe, value, tb):
        if self.cookiefile:
            os.unlink(self.cookiefile)
        if self.crl:
            self.crl.close()
        self.logged_in = None

    def whoami(self):
        if self.logged_in:
            return self.logged_in
        return 'Not logged in'

    def get_realms(self):
        descriptor = self.__get_descriptor()
        return tuple([*descriptor['realms']])

    def get_metrics(self, realm):
        self.__assert_str('realm', realm)
        return self.__get_descriptor_data_frame(realm, 'metrics')

    def __assert_str(self, name, value):
        if not isinstance(value, str):
            raise TypeError(name + ' ' + str(value) +
                            ' must be of type ' + str(str) +
                            ' not ' + str(type(value)))

    def __get_descriptor_data_frame(self, realm, key):
        df = self.__get_indexed_data_frame(
            data=self.__get_descriptor_id_text_info_list(realm, key),
            columns=('id', 'label', 'description'),
            index='id')
        return df

    def __get_indexed_data_frame(self, data, columns, index):
        df = pd.DataFrame(data=data, columns=columns)
        df = df.set_index('id')
        return df

    def get_dimensions(self, realm):
        return self.__get_descriptor_data_frame(realm, 'dimensions')

    def __get_descriptor(self):
        if self.descriptor:
            return self.descriptor

        response = self.__request_json('/controllers/metric_explorer.php',
                                       {'operation': 'get_dw_descripter'})

        if response['totalCount'] != 1:
            raise RuntimeError('Retrieving XDMoD data descriptor')

        self.descriptor = response['data'][0]

        return self.descriptor

    def compliance(self, timeframe):
        """ retrieve compliance reports """

        response = self.__request_json('/controllers/compliance.php',
                                       {'timeframe_mode': timeframe})

        return response

    def resources(self):
        names = []
        types = []
        resource_ids = []

        cdata = self.compliance('to_date')
        for resource in cdata['metaData']['fields']:
            if resource['name'] == 'requirement':
                continue
            names.append(resource['header'][:-7].split('>')[1].replace('-', ' '))
            types.append(resource['status'].split('|')[0].strip())
            resource_ids.append(resource['resource_id'])

        return pd.Series(data=types, index=names)

    def __get_descriptor_id_text_info_list(self, realm, key):
        self.__assert_str('realm', realm)
        self.__assert_str('key', key)
        descriptor = self.__get_descriptor()
        try:
            realm_desc = descriptor['realms'][realm]
        except KeyError:
            raise KeyError('Invalid realm \'' + realm + '\'. ' +
                           'Valid realms are ' + str(self.get_realms())) from None
        try:
            data = realm_desc[key]
        except KeyError:
            raise KeyError('Invalid key \'' + key + '\'') from None
        return [(id,
                 data[id]['text'],
                 data[id]['info']) for id in data]

    def get_dataset(self,
                    start='2022-12-01',
                    end='2022-12-31',
                    realm='Jobs',
                    metric='CPU Hours: Total',
                    dimension='None',
                    filters={},
                    dataset_type='timeseries',
                    aggregation_unit='Auto'):

        config = {
            'start_date': start,
            'end_date': end,
            'realm': realm,
            'statistic': metric,
            'group_by': dimension,
            'public_user': 'true',
            'timeframe_label': '2016',
            'scale': '1',
            'aggregation_unit': aggregation_unit,
            'dataset_type': dataset_type,
            'thumbnail': 'n',
            'query_group': 'po_usage',
            'display_type': 'line',
            'combine_type': 'side',
            'limit': '10',
            'offset': '0',
            'log_scale': 'n',
            'show_guide_lines': 'y',
            'show_trend_line': 'y',
            'show_percent_alloc': 'n',
            'show_error_bars': 'y',
            'show_aggregate_labels': 'n',
            'show_error_labels': 'n',
            'show_title': 'y',
            'width': '916',
            'height': '484',
            'legend_type': 'bottom_center',
            'font_size': '3',
            'inline': 'n',
            'operation': 'get_data',
            'format': 'csv'
        }

        response = self.get_usagedata(config)

        csvdata = csv.reader(response.splitlines())

        if dataset_type == 'aggregate':
            return self.xdmodcsvtopandas(csvdata)
        else:
            labelre = re.compile(r'\[([^\]]+)\].*')
            timestamps = []
            data = []
            for line_num, line in enumerate(csvdata):
                if line_num == 1:
                    title = line[0]
                elif line_num == 5:
                    start, end = line
                elif line_num == 7:
                    dimensions = []
                    for label in line[1:]:
                        match = labelre.match(label)
                        if match:
                            dimensions.append(html.unescape(match.group(1)))
                        else:
                            dimensions.append(html.unescape(label))
                elif line_num > 7 and len(line) > 1:
                    if re.match(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$', line[0]):
                        timestamps.append(datetime.strptime(line[0], '%Y-%m-%d'))
                        data.append(numpy.asarray(line[1:], dtype=numpy.float64))
                    elif re.match(r'^[0-9]{4}-[0-9]{2}$', line[0]):
                        timestamps.append(datetime.strptime(line[0], '%Y-%m'))
                        data.append(numpy.asarray(line[1:], dtype=numpy.float64))
                    elif re.match(r'^[0-9]{4} Q[0-9]$', line[0]):
                        year, quarter = line[0].split(' ')
                        dstamp = ''
                        if quarter == 'Q1':
                            dstamp = year + '-01-01'
                        elif quarter == 'Q2':
                            dstamp = year + '-04-01'
                        elif quarter == 'Q3':
                            dstamp = year + '-07-01'
                        elif quarter == 'Q4':
                            dstamp = year + '-10-01'
                        else:
                            raise Exception('Unsupported date quarter specification ' + line[0])

                        timestamps.append(datetime.strptime(dstamp, '%Y-%m-%d'))
                        data.append(numpy.asarray(line[1:], dtype=numpy.float64))
                    else:
                        # TODO handle other date cases
                        raise Exception('Unsupported date specification ' + line[0])

            return pd.DataFrame(data=data, index=pd.Series(data=timestamps, name='Time'), columns=dimensions)

    def get_usagedata(self, config):
        response = self.__request('/controllers/user_interface.php',
                                  config)

        return response

    def rawdata(self, realm, start, end, filters, stats):
        config = {
            'realm': realm,
            'start_date': start,
            'end_date': end,
            'params': filters,
            'stats': stats
        }

        request = json.dumps(config)

        headers = self.headers + ['Accept: application/json',
                                  'Content-Type: application/json',
                                  'charset: utf-8']

        result = self.__request_json('/rest/v1/warehouse/rawdata',
                                     request,
                                     headers,
                                     contentType='JSON')

        return pd.DataFrame(result['data'], columns=result['stats'], dtype=numpy.float64)

    def xdmodcsvtopandas(self, rd):
        groups = []
        data = []
        for line_num, line in enumerate(rd):
            if line_num == 5:
                start, end = line
            elif line_num == 7:
                group, metric = line
            elif line_num > 7 and len(line) > 1:
                groups.append(html.unescape(line[0]))
                data.append(numpy.float64(line[1]))

        if len(data) == 0:
            return pd.Series(dtype='float64')

        return pd.Series(data=data, index=groups, name=metric)

    def get_qualitydata(self, params, is_numpy=False):
        type_to_title = {'gpu': '% of jobs with GPU information',
                         'hardware': '% of jobs with hardware perf information',
                         'cpu': '% of jobs with cpu usage information',
                         'script': '% of jobs with Job Batch Script information',
                         'realms': '% of jobs in the SUPReMM realm compared to Jobs realm'}

        response = self.__request_json('/rest/supremm_dataflow/quality',
                                       params)

        if response['success']:
            jobs = [job for job in response['result']]
            dates = [date.strftime('%Y-%m-%d') for date in pd.date_range(params['start'], params['end'], freq='D').date]

            quality = numpy.empty((len(jobs), len(dates)))

            for i in range(len(jobs)):
                for j in range(len(dates)):
                    if response['result'][jobs[i]].get(dates[j], numpy.nan) != 'N/A':
                        quality[i, j] = response['result'][jobs[i]].get(dates[j], numpy.nan)
                    else:
                        quality[i, j] = numpy.nan
            if is_numpy:
                return quality
            df = pd.DataFrame(data=quality, index=jobs, columns=dates)
            df.name = type_to_title[params['type']]
            return df
