import string

TRICKY_CHARS = 'IlO' # can be confused with digits
CODE_KEYSPACE_LETTERS = ''.join(set(string.ascii_letters).difference(TRICKY_CHARS))
CODE_KEYSPACE = string.digits + CODE_KEYSPACE_LETTERS

import calendar

MONTH_NAME_ABBR_REGEX = '|'.join(calendar.month_name[1:]+calendar.month_abbr[1:])
MONTH_ABBR_REGEX = '|'.join(calendar.month_abbr[1:])
MONTH_NAME_TO_ABBR = dict(zip(calendar.month_name[1:], calendar.month_abbr[1:]))

import random

def make_random_code(code_length=8): # generated code always starts with a letter, to allow use as identifier
    return ''.join([random.choice(CODE_KEYSPACE_LETTERS)]+[ random.choice(CODE_KEYSPACE) for i in range(code_length-1)])

def gen_random_names(num_names=1):
    first_names=('John','Paul', 'Phillip', 'Alan', 'Mark', 'Emmanuel', 'Kiiza', 'Peace', 'Dorothy', 'Mary', 'Harry', 'Richard', 'Winifred', 'Kato', 'Yusuf', 'James', 'Esther', 'Charles')
    last_names=('Otim', 'Mukasa', 'Businge', 'Musuya', 'Magezi', 'Tandeka', 'Byaruhanga', 'Mwebesa', 'Kasingye', 'Kyobe', 'Walusimbi', 'Odongo', 'Sempagala', 'Mukyaya', 'Gatusha', 'Iloket', 'Kiwanuka', 'Ruvuza', 'Senteza', 'Tumwine', 'Wako', 'Zzime')

    for _ in range(num_names):
        yield (random.choice(first_names), random.choice(last_names))

def period_to_dates(period_str):
    import re
    from datetime import datetime, date

    # Oct to Dec 2016' => ('Oct' '2016') => (2016, 10)
    # m = re.match(r'(%s)(\s+to\s%s)\s*([\d]{4})' % (MONTH_ABBR_REGEX, MONTH_ABBR_REGEX), period_str)
    m = re.match(r'(%s)\s+to\s+(%s)\s*([\d]{4})' % (MONTH_ABBR_REGEX, MONTH_ABBR_REGEX), period_str)
    if m:
        m_name, _, y = m.groups()
        if m_name in MONTH_NAME_TO_ABBR.values():
            m_abbr = m_name
        else:
            m_abbr = MONTH_NAME_TO_ABBR[m_name]
        dt = datetime.strptime(m_abbr + ' ' + y, '%b %Y').date()
        return dt, dt, None

    # 'October 2016' 'Oct 2016' => ('Oct' '2016') => (2016, 10)
    m = re.match(r'(%s)\s*([\d]{4})' % (MONTH_NAME_ABBR_REGEX,), period_str)
    if m:
        m_name, y = m.groups()
        m_abbr = MONTH_NAME_TO_ABBR[m_name]
        dt = datetime.strptime(m_abbr + ' ' + y, '%b %Y').date()
        return dt, dt, dt
    # '2016-Q4' '2016Q4' => (2016, 4) => (2016, 10)
    m = re.match(r'([\d]{4})(-?[Qq]([1-4]))', period_str)
    if m:
        y, _, q = m.groups()
        dt = date(int(y), (int(q)-1)*3+1, 1)
        return dt, dt, None
    # '2016' '2016-10' => (2016) (2016, 10)
    m = re.match(r'([\d]{4})(-([\d]{2}))?', period_str)
    if m:
        y, _, m_str = m.groups()
        if m_str:
            dt = date(int(y), int(m_str), 1)
            return dt, dt, dt
        else:
            dt = date(int(y), 1, 1)
            return dt, None, None

    return None, None, None

def dates_to_iso_periods(ydate, qdate, mdate):
    if ydate:
        iso_month_first = ydate.isoformat()
        iso_month = iso_month_first[:7]
        iso_year = iso_month_first[:4]
        iso_quarter = '%d-Q%d' % (ydate.year, ((ydate.month-1)//3)+1)

        if mdate:
            return iso_year, iso_quarter, iso_month
        if qdate:
            return iso_year, iso_quarter, None
        if ydate:
            return iso_year, None, None

def rasterize(rows, columns, values, row_index_func, col_index_func, default_func=lambda r, c: None):
    """
    Take sparse value sequence and go through the coordinate space filling each
    address with a value from the sequence of values or a computed default
    """
    i_values = iter(values)
    for i, row in enumerate(rows):
        for j, col in enumerate(columns):
            if i == 0 and j == 0:
                curr_val = next(i_values, default_func(row, col))
            if curr_val is not None and row == row_index_func(curr_val) and col == col_index_func(curr_val):
                yield curr_val
                curr_val = next(i_values, default_func(row, col))
            else:
                yield default_func(row, col)

def default(*args, fillvalue=None):
    try:
        return next(filter(lambda x: x is not None, args))
    except StopIteration as e:
        return fillvalue

def default_zero(*args):
    return default(*args, fillvalue=0)

def all_not_none(*args):
    return all(map(lambda x: x is not None, args))
