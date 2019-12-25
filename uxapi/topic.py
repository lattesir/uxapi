from typing import NamedTuple


class UXTopic(NamedTuple):
    exchange_id: str
    market_type: str
    datatype: str
    extrainfo: str = ''

    def __str__(self):
        if self.extrainfo:
            return f'{self.datatype}:{self.extrainfo}'
        else:
            return self.datatype

    @property
    def maintype(self):
        _maintype, *_ = self.datatype.split('.')
        return _maintype

    @property
    def subtypes(self):
        _, *_subtypes = self.datatype.split('.')
        return _subtypes