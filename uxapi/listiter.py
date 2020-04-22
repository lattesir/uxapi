class listiter:
    def __init__(self, lst):
        self.lst = lst
        self.cursor = 0 if lst else -1

    def has_next(self):
        return 0 <= self.cursor < len(self.lst)

    def __iter__(self):
        return self

    def __next__(self):
        if self.has_next():
            self.cursor += 1
            return self.lst[self.cursor - 1]
        raise StopIteration

    def rewind(self):
        self.cursor = 0 if self.lst else -1

    def prepend(self, elem):
        self.lst.insert(0, elem)
        self.cursor += 1

    def append(self, elem):
        self.lst.append(elem)
        if self.cursor < 0:
            self.cursor = 0

    def add(self, elem):
        i = max(self.cursor, 0)
        self.lst.insert(i, elem)
        self.cursor += 1

    def remove(self, elem=None):
        if elem is None:
            i = self.cursor - 1
        else:
            i = self.lst.index(elem)

        if i < 0:
            raise IndexError('no previous element')

        self.lst.pop(i)
        if self.cursor > 0:
            if i < self.cursor:
                self.cursor -= 1
        else:
            if not self.lst:
                self.cursor = -1

    def set(self, new_elem):
        if self.cursor > 0:
            self.lst[self.cursor - 1] = new_elem
        raise IndexError('no previous element')
