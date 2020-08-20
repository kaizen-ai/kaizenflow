import enum


class Frequency(enum.Enum):
    Minutely = "T"
    Daily = "D"


class ContractType(enum.Enum):
    Continuous = "continuous"
    Expiry = "expiry"
