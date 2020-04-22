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
        args = ', '.join(repr(field) for field in tuple(self))
        return f'UXTopic({args})'

    def __str__(self):
        s = ':'.join(self)
        return s if self.extrainfo else s[:-1]

    def __eq__(self, other):
        if isinstance(other, UXTopic):
            return tuple(self) == tuple(other)
        return False

    def __hash__(self):
        return hash(str(self))

    def __iter__(self):
        fields = (
            self.exchange_id,
            self.market_type,
            self.datatype,
            self.extrainfo
        )
        return (field for field in fields)

    @property
    def maintype(self):
        _maintype, *_ = self.datatype.split('.')
        return _maintype

    @property
    def subtypes(self):
        _, *_subtypes = self.datatype.split('.')
        return _subtypes
