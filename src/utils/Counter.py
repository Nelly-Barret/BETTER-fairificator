class Counter:
    count = 0

    @classmethod
    def incr(cls):
        cls.count += 1
        return cls.count

    @classmethod
    def set(cls, new_value):
        cls.count = new_value

    def __init__(self, new_value: int = -1):
        if new_value != -1:
            self.set(new_value)
        else:
            self.id = self.incr()
