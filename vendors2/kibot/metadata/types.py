import dataclasses
import enum

# TODO(Amr): add doc strings to these classes with examples.


class KibotContractType(enum.Enum):
    Daily = "Daily"
    OneMin = "1Min"
    TickBidAsk = "TickBidAsk"
    Continuos = "Continuous"


@dataclasses.dataclass
class ContractMetadata:
    Symbol: str
    Link: str
    Description: str


@dataclasses.dataclass
class TickBidAskContractMetadata:
    SymbolBase: str
    Symbol: str
    StartDate: str
    Size: str
    Description: str
    Exchange: str


@dataclasses.dataclass
class Adjustment:
    Date: str
    Symbol: str
    Company: str
    Action: str
    Description: str
    EventDate: str


@dataclasses.dataclass
class Ticker:
    Symbol: str
    StartDate: str
    Size: str
    Description: str
    Exchange: str
    Industry: str
    Sector: str
