import csv
from datetime import datetime
import html
import numpy as np
import pandas as pd
import re


def _process_get_data_response(dw, params, response):
    csv_data = csv.reader(response.splitlines())
    if not params['timeseries']:
        return __xdmod_csv_to_pandas(params, csv_data)
    else:
        label_re = re.compile(r'\[([^\]]+)\].*')
        time_values = []
        data = []
        for line_num, line in enumerate(csv_data):
            if line_num == 5:
                start_date, end_date = line
            elif line_num == 7:
                dimension_values = []
                for label in line[1:]:
                    match = label_re.match(label)
                    if match:
                        dimension_values.append(html.unescape(match.group(1)))
                    else:
                        dimension_values.append(html.unescape(label))
            elif line_num > 7 and len(line) > 1:
                date_string = line[0]
                # Match YYYY-MM-DD
                if re.match(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$', line[0]):
                    format_ = '%Y-%m-%d'
                # Match YYYY-MM
                elif re.match(r'^[0-9]{4}-[0-9]{2}$', line[0]):
                    format_ = '%Y-%m'
                # Match YYYY
                elif re.match(r'^[0-9]{4}$', line[0]):
                    format_ = '%Y'
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
                    format_ = '%Y-%m-%d'
                else:
                    raise Exception(
                        'Unsupported date specification ' + line[0] + '.'
                    )
                time_values.append(datetime.strptime(date_string, format_))
                data.append(np.asarray(line[1:])
        return pd.DataFrame(
            data=data,
            index=pd.Series(data=time_values, name='Time'),
            columns=pd.Series(
                dimension_values,
                name=dw._get_dimension_label(
                    params['realm'], params['dimension']
                )
            )
        )


def __xdmod_csv_to_pandas(params, csv_data):
    dimension_values = []
    data = []
    for line_num, line in enumerate(csv_data):
        if line_num > 7 and len(line) > 1:
            dimension_values.append(html.unescape(line[0]))
            data.append(line[1])
    if len(data) == 0:
        return pd.Series()
    return pd.Series(
        data=data,
        name=params['metric'],
        index=pd.Series(data=dimension_values, name=params['dimension']),
    )
