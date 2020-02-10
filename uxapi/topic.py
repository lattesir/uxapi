class UXTopic:
    def __init__(self, exchange_id, market_type, datatype, extrainfo=''):
        self.exchange_id = exchange_id
        self.market_type = market_type
        self.datatype = datatype
        self.extrainfo = extrainfo

    @classmethod
    def fromstring(cls, s):
        return cls(*s.split(':'))

    def __repr__(self):
        args = ', '.join(repr(f) for f in self.fields)
        return f'UXTopic({args})'

    def __str__(self):
        s = ':'.join(self.fields)
        return s if self.extrainfo else s[:-1]

    def __eq__(self, other):
        if isinstance(other, UXTopic):
            return self.fields == other.fields
        return False

    def __hash__(self):
        return hash(str(self))

    @property
    def fields(self):
        return (self.exchange_id, self.market_type,
                self.datatype, self.extrainfo)

    @property
    def maintype(self):
        _maintype, *_ = self.datatype.split('.')
        return _maintype

    @property
    def subtypes(self):
        _, *_subtypes = self.datatype.split('.')
        return _subtypes