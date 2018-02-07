import re
from datetime import date, timedelta

def next_quarter(quarter_year, quarter_num):
    """
    >>> next_quarter(2015, 1), next_quarter(2015, 2), next_quarter(2015, 3), next_quarter(2015, 4)
    ((2015, 2), (2015, 3), (2015, 4), (2016, 1))

    """
    next_quarter_year = quarter_year + quarter_num // 4
    next_quarter_num = quarter_num % 4 + 1
    return next_quarter_year, next_quarter_num

class FormatError(Exception):
    """ Raised when a string describing a quarter is not in the ISO 8601 format """
    pass

class Quarter():
    """
    >>> Quarter.from_str('2014Q2')
    Quarter(2014, 2)

    >>> Quarter.from_str('2011-Q4')
    Quarter(2011, 4)

    >>> Quarter.from_str('2016Q0')
    Traceback (most recent call last):
    dateutil.FormatError: Quarter not specified in ISO 8601 format (YYYYQN or YYYY-QN): 2016Q0

    >>> Quarter.from_str('2016Q3') == Quarter.from_str('2016-Q3')
    True

    """

    def __init__(self, q_year, q_num):
        self.year = q_year
        self.qnum = q_num

    @classmethod
    def from_str(cls, iso_quarter):
        m = re.match(r'(\d\d\d\d)-?Q([1234])', iso_quarter)
        if m is None:
            raise FormatError('Quarter not specified in ISO 8601 format (YYYYQN or YYYY-QN): ' + iso_quarter)
        (match_year, match_num) = m.groups()
        return cls(int(match_year), int(match_num))

    def next(self):
        next_year, next_qnum = next_quarter(self.year, self.qnum)
        return Quarter(next_year, next_qnum)

    def start_date(self):
        return date(self.year, (self.qnum-1)*3+1, 1)

    def end_date(self):
        return self.next().start_date() - timedelta(days=1)

    def iter_until(self, end_quarter):
        current_quarter = self
        while current_quarter <= end_quarter:
            yield current_quarter
            current_quarter = current_quarter.next()

    def __eq__(self, other):
        return (self.year == other.year) and (self.qnum == other.qnum)

    def __lt__(self, other):
        """
        >>> Quarter.from_str('2015Q1') < Quarter.from_str('2015Q3')
        True

        >>> Quarter.from_str('2015Q1') < Quarter.from_str('2014Q3')
        False

        """
        return (self.year*10+self.qnum) < (other.year*10+other.qnum)
    def __le__(self, other):
        return self.__eq__(other) or self.__lt__(other)
    def __gt__(self, other):
        return not self.__ge__(other)
    def __ge__(self, other):
        return not self.__lt__(other)

    def __repr__(self):
        return 'Quarter(%d, %d)' % (self.year, self.qnum)

    def __str__(self):
        return '%d-Q%d' % (self.year, self.qnum)

def get_quarters(quarter_start, quarter_end):
    """
    >>> get_quarters('2015Q1', '2016Q1')
    ['2015Q1', '2015Q2', '2015Q3', '2015Q4', '2016Q1']

    >>> get_quarters('2016Q1', '2015Q1')
    ['2016Q1', '2015Q1']

    """
    s = Quarter.from_str(quarter_start) # '2012Q4' => (2012, 4)
    e = Quarter.from_str(quarter_end)

    if (s < e):
        q_list = [str(q) for q in s.iter_until(e)]
    else:
        q_list = [quarter_start, quarter_end]

    return q_list

def iso_quarter_to_dates(iso_quarter):
    """
    >>> [iso_quarter_to_dates(q) for q in ('2015Q1', '2015Q2', '2015Q3', '2015Q4', '2016Q1',)]
    [(datetime.date(2015, 1, 1), datetime.date(2015, 3, 31)), (datetime.date(2015, 4, 1), datetime.date(2015, 6, 30)), (datetime.date(2015, 7, 1), datetime.date(2015, 9, 30)), (datetime.date(2015, 10, 1), datetime.date(2015, 12, 31)), (datetime.date(2016, 1, 1), datetime.date(2016, 3, 31))]

    >>> [(s.isoformat(), e.isoformat()) for (s,e) in [iso_quarter_to_dates(q) for q in ('2015Q1', '2015Q2', '2015Q3', '2015Q4', '2016Q1',)]]
    [('2015-01-01', '2015-03-31'), ('2015-04-01', '2015-06-30'), ('2015-07-01', '2015-09-30'), ('2015-10-01', '2015-12-31'), ('2016-01-01', '2016-03-31')]

    """

    s_quarter = Quarter.from_str(iso_quarter)
    start_date = s_quarter.start_date()
    return (s_quarter.start_date(), s_quarter.end_date())

class DateSpan():
    def __init__(self, start_date, end_date):
        self.start = start_date
        self.end = end_date

    @classmethod
    def fromquarter(cls, quarter_str):
        return cls(*iso_quarter_to_dates(quarter_str))

    def combine(self, other):
        """
        >>> DateSpan.fromquarter('2007Q3').combine(DateSpan.fromquarter('2008Q1'))
        DateSpan(datetime.date(2007, 7, 1), datetime.date(2008, 3, 31))

        >>> DateSpan.fromquarter('2007Q3').combine(DateSpan.fromquarter('2007Q1'))
        DateSpan(datetime.date(2007, 1, 1), datetime.date(2007, 9, 30))

        """
        all_dates = sorted((self.start, self.end, other.start, other.end))
        return DateSpan(all_dates[0], all_dates[-1])

    def format_short(self):
        return self.start.strftime('%b%y') + '-' + self.end.strftime('%b%y')

    def format(self):
        return self.start.strftime('%b') + ' to ' + self.end.strftime('%b %Y')

    def format_long(self):
        return self.start.strftime('%B %Y') + ' to ' + self.end.strftime('%B %Y')

    def __eq__(self, other):
        return (self.start==other.start) and (self.end==other.end)

    def __repr__(self):
        return 'DateSpan(%s, %s)' % (repr(self.start), repr(self.end))

    def __str__(self):
        return 'DateSpan(%s, %s)' % (self.start.isoformat(), self.end.isoformat())
