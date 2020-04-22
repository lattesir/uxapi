import time
import hmac as _hmac
import base64
import collections
import itertools
from operator import itemgetter
import pendulum


def current_timestamp():
    return int(time.time()) * 1000


def is_sorted(iterable, *, key=None, reverse=False):
    it1, it2 = itertools.tee(iterable)
    next(it2, None)
    if reverse:
        it1, it2 = it2, it1
    for a, b in zip(it1, it2):
        if key:
            a, b = key(a), key(b)
        if a > b:
            return False
    return True


def all_equal(lst):
    return lst[1:] == lst[:-1]


def extend(*args):
    result = {}
    if args:
        if isinstance(type(args[0]), collections.OrderedDict):
            result = collections.OrderedDict()
        for arg in args:
            if arg:
                result.update(arg)
    return result


def deep_extend(*args):
    result = None
    for arg in args:
        if isinstance(arg, dict):
            if not isinstance(result, dict):
                result = {}
            for key in arg:
                result[key] = deep_extend(result.get(key), arg[key])
        else:
            result = arg
    return result


def keysort(d):
    return collections.OrderedDict(sorted(d.items(), key=itemgetter(0)))


def to_timestamp(text, **options):
    dt = pendulum.parse(text, **options)
    return dt.timestamp() * 1000


def hmac(secret, msg, algorithm='sha256', digest='base64'):
    h = _hmac.new(secret, msg, algorithm)
    if digest == 'hex':
        return h.hexdigest()
    elif digest == 'base64':
        return base64.b64encode(h.digest())
    return h.digest()


def contract_delivery_time(expiration, delivery_hour, since=None):
    """返回合约交割时间

    :param expiration: 当周合约('CW')，次周合约('NW'), 季度合约('CQ')
    :param delivery_hour: 合约交割时间(UTC)
    :param since: 参照时间(UTC), datetime类型
    :return: 合约交割时间, datetime类型
    """
    since = since or pendulum.now('UTC')
    since = pendulum.instance(since)

    if expiration == 'CW':
        cw = since.start_of('week').add(days=4).add(hours=delivery_hour)
        if since > cw:
            cw = cw.next(pendulum.FRIDAY, keep_time=True)
        return cw

    if expiration == 'NW':
        cw = contract_delivery_time('CW', delivery_hour, since)
        return cw.next(pendulum.FRIDAY, keep_time=True)

    if expiration == 'CQ':
        last_friday = since.last_of('quarter', pendulum.FRIDAY)
        last_friday = last_friday.add(hours=delivery_hour)
        if since >= last_friday.subtract(weeks=2):
            since = start_of('next_quarter', since)
            return contract_delivery_time('CQ', delivery_hour, since)
        else:
            return last_friday

    if expiration == 'NQ':
        cq = contract_delivery_time('CQ', delivery_hour, since)
        since = start_of('next_quarter', cq)
        return contract_delivery_time('CQ', delivery_hour, since)

    raise ValueError('invalid expiration')


_PENDULUM_UNITS = ['day', 'week', 'month', 'year', 'decade', 'century']
_EXTENDED_UNITS = [
    'previous_week', 'next_week',
    'previous_month', 'next_month',
    'previous_quarter', 'quarter', 'next_quarter',
]


def start_of(unit, dt):
    if unit not in (_PENDULUM_UNITS + _EXTENDED_UNITS):
        raise ValueError(f'Invalid unit "{unit}" for start_of()')
    dt = pendulum.instance(dt)
    if unit in _PENDULUM_UNITS:
        return dt.start_of(unit)
    if unit == 'quarter':
        start_month = (dt.quarter - 1) * 3 + 1
        return dt.set(month=start_month).start_of('month')
    previous_or_next, unit = unit.split('_')
    if previous_or_next == 'previous':
        previous_day = start_of(unit, dt).subtract(days=1)
        return start_of(unit, previous_day)
    else:
        next_day = end_of(unit, dt).add(days=1)
        return start_of(unit, next_day)


def end_of(unit, dt):
    if unit not in (_PENDULUM_UNITS + _EXTENDED_UNITS):
        raise ValueError(f'Invalid unit "{unit}" for end_of()')
    dt = pendulum.instance(dt)
    if unit in _PENDULUM_UNITS:
        return dt.end_of(unit)
    if unit == 'quarter':
        end_month = dt.quarter * 3
        return dt.set(month=end_month).end_of('month')
    previous_or_next, unit = unit.split('_')
    if previous_or_next == 'previous':
        previous_day = start_of(unit, dt).subtract(days=1)
        return end_of(unit, previous_day)
    else:
        next_day = end_of(unit, dt).add(days=1)
        return end_of(unit, next_day)
