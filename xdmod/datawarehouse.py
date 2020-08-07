import numpy
import csv
import pycurl
from urllib.parse import urlencode
import pandas as pd
import io

class DataWareHouse:
    def __init__(self, xdmodhost, apikey):
        self.xdmodhost = xdmodhost
        self.apikey = apikey

    def aggregate(self, realm, group_by, statistic, start, end):

        config = {
            'start_date': start,
            'end_date': end,
            'realm': realm,
            'statistic': statistic,
            'group_by': group_by,
            'public_user': 'true',
            'timeframe_label': '2016',
            'scale': '1',
            'aggregation_unit': 'Auto',
            'dataset_type': 'aggregate',
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

        return self.xdmodcsvtopandas(csvdata)

    def get_usagedata(self, config):

        crl = pycurl.Curl()
        crl.setopt(crl.URL, self.xdmodhost + '/controllers/user_interface.php')
        pf = urlencode(config)
        b_obj = io.BytesIO()
        crl.setopt(crl.WRITEDATA, b_obj)
        crl.setopt(crl.POSTFIELDS, pf)
        crl.perform()
        crl.close()

        get_body = b_obj.getvalue()

        return get_body.decode('utf8')

    def xdmodcsvtopandas(self, rd):
        groups = []
        data = []
        for line_num, line in enumerate(rd):
            if line_num == 1:
                title = line[0]
            elif line_num == 5:
                start, end = line
            elif line_num == 7:
                group, metric = line
            elif line_num > 7 and len(line) > 1:
                groups.append(line[0])
                data.append(numpy.float64(line[1]))

        return pd.DataFrame(data=data, index=groups)
