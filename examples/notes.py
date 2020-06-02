class OkexOrderBookDeltaToRedisTimeSeries:
    def __init__(self, rts, buckets):
        self.buckets = buckets
        self.b_num = 0
        self.rts = rts
        self.retention_msecs = 60000

    def __call__(self, delta):
        price_key = f"{delta['key']}:price"
        size_key = f"{delta['key']}:size"
        liq_num_key = f"{delta['key']}:liq_num"
        ord_num_key = f"{delta['key']}:ord_num"
        labels = {
            'event': delta['event'],
            'ts': delta['ts'],
            'exchange': delta['exchange'],
            'market_type': delta['market_type'],
            'symbol': delta['symbol'],
            'checksum': delta['info']['data'][0]['checksum'],
        }

        for side in ['asks', 'bids']:
            print(delta['ts'], side, self.b_num, len(delta[side]))
            for i, offer in enumerate(delta[side]):
                ts = delta['ts'] + i
                self.rts.add(f"{price_key}:{side}:{self.b_num}", ts, offer[0], retention_msecs=self.retention_msecs, labels={**labels, **{'column': 'price'}})
                self.rts.add(f"{size_key}:{side}:{self.b_num}", ts, offer[1], retention_msecs=self.retention_msecs, labels={**labels, **{'column': 'size'}})
                self.rts.add(f"{liq_num_key}:{side}:{self.b_num}", ts, offer[2], retention_msecs=self.retention_msecs, labels={**labels, **{'column': 'liq_num'}})
                self.rts.add(f"{ord_num_key}:{side}:{self.b_num}", ts, offer[3], retention_msecs=self.retention_msecs, labels={**labels, **{'column': 'ord_num'}})
        # bucket number count roll over
        self.b_num += 1
        self.b_num = (self.b_num + self.buckets) % self.buckets
        return delta


class HuobiOrderBookDeltaToRedisTimeSeries:
    def __init__(self, rts, buckets):
        self.buckets = buckets
        self.b_num = 0
        self.rts = rts

    def __call__(self, delta):
        price_key = f"{delta['key']}:price"
        size_key = f"{delta['key']}:size"
        labels = delta.copy()
        for l in {'asks', 'bids', 'key', 'info'}:
            labels.pop(l)
        for l in {'id', 'mrid', 'version'}:
            labels[l] = delta['info']['tick'][l]
        for side in ['asks', 'bids']:
            print(delta['ts'], side, labels['version'], len(delta[side]))
            for i, offer in enumerate(delta[side]):
                self.rts.add(f"{price_key}:{side}:{self.b_num}", delta['ts']+i, offer[0], retention_msecs=60000, labels=labels)
                self.rts.add(f"{size_key}:{side}:{self.b_num}", delta['ts']+i, offer[1], retention_msecs=60000, labels=labels)
        # bucket number count roll over
        self.b_num += 1
        self.b_num = (self.b_num + self.buckets) % self.buckets
        return delta
