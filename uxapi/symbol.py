class UXSymbol:
    def __init__(self, exchange_id, market_type, name):
        self.exchange_id = exchange_id
        self.market_type = market_type
        self.name = name
        self.isspecial = self.name.startswith('!')
        if self.isspecial:
            self._name_info = name[1:].split('.', maxsplit=2)
        else:
            self._name_info = name.split('.', maxsplit=2)

    @classmethod
    def fromstring(cls, s):
        return cls(*s.split(':'))

    def __repr__(self):
        args = ', '.join(repr(field) for field in tuple(self))
        return f'UXSymbol({args})'

    def __str__(self):
        return ':'.join(self)

    def __eq__(self, other):
        if isinstance(other, UXSymbol):
            return tuple(self) == tuple(other)
        return False

    def __hash__(self):
        return hash(str(self))
    
    def __iter__(self):
        return (field for field in (self.exchange_id, self.market_type, self.name))

    @property
    def base_quote(self):
        assert '/' in self._name_info[0], 'invalid format'
        base, quote = self._name_info[0].split('/', maxsplit=1)
        return base, quote

    @property
    def base(self):
        return self.base_quote[0]

    @property
    def quote(self):
        return self.base_quote[1]

    @property
    def contract_expiration(self):
        assert len(self._name_info) >= 2, 'invalid format'
        return self._name_info[1]