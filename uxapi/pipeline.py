class Pipeline:
    def __init__(self, processors=None):
        self.processors = processors or []

    def __call__(self, v):
        for processor in self.processors:
            v = processor(v)