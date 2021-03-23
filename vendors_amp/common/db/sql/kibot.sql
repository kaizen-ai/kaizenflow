CREATE TABLE IF NOT EXISTS DailyData (
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

CREATE TABLE IF NOT EXISTS MinuteData (
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

CREATE TABLE IF NOT EXISTS TickBidAskData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES TradeSymbol,
    datetime timestamp,
    bid numeric,
    ask numeric,
    volume bigint
);

CREATE TABLE IF NOT EXISTS TickData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES TradeSymbol,
    datetime timestamp,
    price numeric,
    size bigint
);

INSERT INTO Exchange (name) VALUES ('TestExchange');
