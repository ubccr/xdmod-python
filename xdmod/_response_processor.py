import csv
from datetime import datetime
import html
import numpy as np
import pandas as pd
import re


def _process_get_data_response(dw, params, response):
    csvdata = csv.reader(response.splitlines())
    if not params['timeseries']:
        return __xdmod_csv_to_pandas(csvdata)
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
                name=dw._get_dimension_label(
                    params['realm'], params['dimension']
                )
            )
        )


def __xdmod_csv_to_pandas(csv_data):
    groups = []
    data = []
    for line_num, line in enumerate(csv_data):
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
