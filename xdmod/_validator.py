from datetime import date, timedelta


def _assert_str(name, value):
    return __assert_type(name, value, str, 'string')


def _assert_runtime_context(in_runtime_context):
    if not in_runtime_context:
        raise RuntimeError(
            'Method is being called outside of the runtime context.'
            + ' Make sure this method is only called within the body'
            + ' of a `with` statement.'
        )


def _validate_get_data_params(data_warehouse, descriptors, params):
    results = {}
    (results['start_date'], results['end_date']) = (
        __validate_duration(params['duration'])
    )
    results['realm'] = _find_realm_id(descriptors, params['realm'])
    results['metric'] = __find_metric_id(
        descriptors,
        results['realm'],
        params['metric'],
    )
    results['dimension'] = _find_dimension_id(
        descriptors,
        results['realm'],
        params['dimension'],
    )
    results['filters'] = __validate_filters(
        data_warehouse,
        descriptors,
        results['realm'],
        params['filters'],
    )
    results['timeseries'] = __assert_bool('timeseries', params['timeseries'])
    results['aggregation_unit'] = __find_str_in_sequence(
        params['aggregation_unit'],
        _get_aggregation_units(),
        'aggregation_unit',
    )
    return results


def _validate_get_raw_data_params(data_warehouse, descriptors, params):
    results = {}
    (results['start_date'], results['end_date']) = (
        __validate_duration(params['duration'])
    )
    results['realm'] = _find_raw_realm_id(descriptors, params['realm'])
    results['fields'] = __validate_raw_fields(
        data_warehouse,
        params['realm'],
        params['fields'],
    )
    results['filters'] = __validate_filters(
        data_warehouse,
        descriptors,
        params['realm'],
        params['filters'],
    )
    results['show_progress'] = __assert_bool(
        'show_progress',
        params['show_progress'],
    )
    return results


def _find_realm_id(descriptors, realm):
    return __find_id_in_descriptor(
        descriptors._get_aggregate(),
        'realm',
        realm,
    )


def _find_dimension_id(descriptors, realm, dimension):
    return __find_metric_or_dimension_id(
        descriptors,
        realm,
        'dimension',
        dimension,
    )


def _get_durations():
    this_year = date.today().year
    six_years_ago = this_year - 6
    last_seven_years = tuple(
        map(str, reversed(range(six_years_ago, this_year + 1)))
    )
    return (
        (
            'Yesterday',
            '7 day',
            '30 day',
            '90 day',
            'Month to date',
            'Previous month',
            'Quarter to date',
            'Previous quarter',
            'Year to date',
            'Previous year',
            '1 year',
            '2 year',
            '3 year',
            '5 year',
            '10 year',
        )
        + last_seven_years
    )


def _get_aggregation_units():
    return (
        'Auto',
        'Day',
        'Month',
        'Quarter',
        'Year',
    )


def _find_raw_realm_id(descriptors, realm):
    return __find_id_in_descriptor(
        descriptors._get_raw(),
        'realm',
        realm,
    )


def __assert_type(name, value, type_, type_name):
    if not isinstance(value, type_):
        raise TypeError('`' + name + '` must be a ' + type_name + '.')
    return value


def __validate_duration(duration):
    if isinstance(duration, str):
        duration = __find_str_in_sequence(
            duration,
            _get_durations(),
            'duration',
        )
        (start_date, end_date) = __get_dates_from_duration(duration)
    else:
        try:
            (start_date, end_date) = duration
        except (TypeError, ValueError) as error:
            raise type(error)(
                '`duration` must be a string or an object'
                + ' with 2 items.'
            ) from None
    return (start_date, end_date)


def __find_metric_id(descriptors, realm, metric):
    return __find_metric_or_dimension_id(
        descriptors,
        realm,
        'metric',
        metric,
    )


def __validate_filters(data_warehouse, descriptors, realm, filters):
    try:
        result = {}
        for dimension in filters:
            dimension_id = _find_dimension_id(descriptors, realm, dimension)
            filter_values = filters[dimension]
            if isinstance(filter_values, str):
                filter_values = [filter_values]
            result[dimension_id] = []
            for filter_value in filter_values:
                new_filter_value = __find_value_in_df(
                    'Filter value',
                    data_warehouse.get_filters(realm, dimension),
                    filter_value,
                )
                result[dimension_id].append(new_filter_value)
        return result
    except TypeError:
        raise TypeError(
            '`filters` must be a mapping whose keys are strings and whose'
            + ' values are strings or sequences of strings.'
        ) from None


def __assert_bool(name, value):
    return __assert_type(name, value, bool, 'Boolean')


def __find_str_in_sequence(value, sequence, label):
    _assert_str(label, value)
    transformed_value = __lowercase_and_remove_spaces(value)
    for valid_value in sequence:
        transformed_valid_value = __lowercase_and_remove_spaces(valid_value)
        if transformed_valid_value == transformed_value:
            return valid_value
    raise KeyError(
        'Invalid value for `' + label + '`: \'' + value + '\''
        + '. Valid values are: \'' + '\', \''.join(sequence) + '\'.'
    ) from None


def __validate_raw_fields(data_warehouse, realm, fields):
    try:
        results = []
        for field in fields:
            new_field = __find_value_in_df(
                'Field',
                data_warehouse.get_raw_fields(realm),
                field,
            )
            results.append(new_field)
        return results
    except TypeError:
        raise TypeError(
            '`fields` must be a sequence of strings.'
        ) from None


def __find_id_in_descriptor(descriptor, name, value):
    _assert_str(name, value)
    for id_ in descriptor:
        if id_ == value or descriptor[id_]['label'] == value:
            return id_
    raise KeyError(
        name.capitalize() + ' \'' + value + '\' not found.'
    )


def __find_metric_or_dimension_id(descriptors, realm, m_or_d, value):
    return __find_id_in_descriptor(
        descriptors._get_aggregate()[realm][m_or_d + 's'],
        m_or_d,
        value,
    )


def __get_dates_from_duration(duration):
    today = date.today()
    yesterday = today + timedelta(days=-1)
    last_week = today + timedelta(days=-7)
    last_month = today + timedelta(days=-30)
    last_quarter = today + timedelta(days=-90)
    this_month_start = date(today.year, today.month, 1)
    if today.month == 1:
        last_full_month_start_year = today.year - 1
        last_full_month_start_month = 12
    else:
        last_full_month_start_year = today.year
        last_full_month_start_month = today.month - 1
    last_full_month_start = date(
        last_full_month_start_year,
        last_full_month_start_month,
        1,
    )
    last_full_month_end = this_month_start + timedelta(days=-1)
    this_quarter_start = date(
        today.year,
        ((today.month - 1) // 3) * 3 + 1,
        1,
    )
    if today.month < 4:
        last_quarter_start_year = today.year - 1
    else:
        last_quarter_start_year = today.year
    last_quarter_start = date(
        last_quarter_start_year,
        (((today.month - 1) - ((today.month - 1) % 3) + 9) % 12) + 1,
        1,
    )
    last_quarter_end = this_quarter_start + timedelta(days=-1)
    this_year_start = date(today.year, 1, 1)
    previous_year_start = date(today.year - 1, 1, 1)
    previous_year_end = date(today.year - 1, 12, 31)
    return {
        'Yesterday': (yesterday, yesterday),
        '7 day': (last_week, today),
        '30 day': (last_month, today),
        '90 day': (last_quarter, today),
        'Month to date': (this_month_start, today),
        'Previous month': (last_full_month_start, last_full_month_end),
        'Quarter to date': (this_quarter_start, today),
        'Previous quarter': (last_quarter_start, last_quarter_end),
        'Year to date': (this_year_start, today),
        'Previous year': (previous_year_start, previous_year_end),
        '1 year': (__date_add_years(today, -1), today),
        '2 year': (__date_add_years(today, -2), today),
        '3 year': (__date_add_years(today, -3), today),
        '5 year': (__date_add_years(today, -5), today),
        '10 year': (__date_add_years(today, -10), today),
    }[duration]


def __find_value_in_df(label, df, value):
    if value in df.index:
        return value
    elif value in df['label'].values:
        return df.index[df['label'] == value].tolist()[0]
    else:
        raise KeyError(label + ' \'' + value + '\' not found.')


def __lowercase_and_remove_spaces(value):
    return value.lower().replace(' ', '')


def __date_add_years(old_date, year_delta):
    # Make dates behave like Ext.JS, i.e., if a date is specified
    # with a day value that is too big, add days to the last valid
    # day in that month, e.g., 2023-02-31 becomes 2023-03-03.
    new_date_year = old_date.year + year_delta
    new_date_day = old_date.day
    days_above = 0
    keep_going = True
    while keep_going:
        try:
            new_date = date(new_date_year, old_date.month, new_date_day)
            keep_going = False
        except ValueError:
            new_date_day -= 1
            days_above += 1
    return new_date + timedelta(days=days_above)
