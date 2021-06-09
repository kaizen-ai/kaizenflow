CREATE TABLE IF NOT EXISTS KibotDailyData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES TradeSymbol,
    date date,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume bigint,
    UNIQUE (trade_symbol_id, date)
);

CREATE TABLE IF NOT EXISTS KibotMinuteData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES TradeSymbol,
    datetime timestamp,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume bigint,
    UNIQUE (trade_symbol_id, datetime)
);

CREATE TABLE IF NOT EXISTS KibotTickBidAskData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES TradeSymbol,
    datetime timestamp,
    bid numeric,
    ask numeric,
    volume bigint
);

CREATE TABLE IF NOT EXISTS KibotTickData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES TradeSymbol,
    datetime timestamp,
    price numeric,
    size bigint
);
