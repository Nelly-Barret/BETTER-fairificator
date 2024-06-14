from src.utils.setup_logger import log


class Counter:
    count = 0

    @classmethod
    def incr(cls):
        cls.count += 1
        return cls.count

    @classmethod
    def set(cls, new_value):
        cls.count = new_value
        return cls.count

    @classmethod
    def reset(cls):
        cls.count = 0
        return cls.count

    def __init__(self, new_value: int = -1):
        if new_value != -1:
            log.debug("set up a new value for the Counter being %s", new_value)
            self.set(new_value)
        else:
            self.id = self.incr()
