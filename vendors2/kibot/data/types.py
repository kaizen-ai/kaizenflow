import enum


class Frequency(enum.Enum):
    Minutely = "T"
    Daily = "D"


class ContractType(enum.Enum):
    Continuous = "continuous"
    Expiry = "expiry"


class Extension(enum.Enum):
    CSV = "csv"
    Parquet = "pq"
