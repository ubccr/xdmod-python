import mariadb
import numpy
import csv
import pycurl
from urllib.parse import urlencode
import pandas as pd

class DataWareHouse:
    def __init__(self, xdmodhost, apikey):
        self.xdmodhost = xdmodhost
        self.con = mariadb.connect(
                user="xdmod-vpn-ro",
                password=apikey,
                host="openxdmod-dev-db.ccr.xdmod.org",
                port=3306,
                database="modw_aggregates")

    def rawdata(self, realm, groupby, statistic, start, end):
        cur = self.con.cursor()
        cur.execute("SELECT jf.application_id, SUM(jf.job_count) AS job_count, SUM(jf.cpu_time) / 3600.0 AS cpu_time FROM modw_aggregates.supremmfact_by_day jf, modw.days d where d.id= jf.day_id and d.day_start BETWEEN '2020-07-01' and '2020-07-31' GROUP BY 1 ORDER BY 3 DESC;")

        data = []
        for res in cur:
            data.append(res[2])

        return numpy.array(data)

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
        b_obj = BytesIO()
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
