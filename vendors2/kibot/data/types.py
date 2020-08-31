import enum


class AssetClass(enum.Enum):
    Futures = "Futures"
    ETFs = "etfs"
    Forex = "forex"
    Stocks = "stocks"


class Frequency(enum.Enum):
    Minutely = "T"
    Daily = "D"
    Tick = "tick"


class ContractType(enum.Enum):
    Continuous = "continuous"
    Expiry = "expiry"


class Extension(enum.Enum):
    CSV = "csv"
    Parquet = "pq"
