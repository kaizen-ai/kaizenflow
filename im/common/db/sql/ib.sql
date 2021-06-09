CREATE TABLE IF NOT EXISTS IbDailyData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES TradeSymbol,
    date date,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume bigint,
    average numeric,
    -- TODO(*): barCount -> bar_count
    barCount integer,
    UNIQUE (trade_symbol_id, date)
);

CREATE TABLE IF NOT EXISTS IbMinuteData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES TradeSymbol,
    datetime timestamptz,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume bigint,
    average numeric,
    barCount integer,
    UNIQUE (trade_symbol_id, datetime)
);

CREATE TABLE IF NOT EXISTS IbTickBidAskData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES TradeSymbol,
    datetime timestamp,
    bid numeric,
    ask numeric,
    volume bigint
);

CREATE TABLE IF NOT EXISTS IbTickData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES TradeSymbol,
    datetime timestamp,
    price numeric,
    size bigint
);
