CREATE TYPE AssetClass AS ENUM ('Futures', 'etfs', 'forex', 'stocks', 'sp_500');
CREATE TYPE Frequency AS ENUM ('T', 'D', 'tick');
CREATE TYPE ContractType AS ENUM ('continuous', 'expiry');
CREATE SEQUENCE serial START 1;

CREATE TABLE IF NOT EXISTS Exchange (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    name text UNIQUE
);

CREATE TABLE IF NOT EXISTS Symbol (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    code text UNIQUE,
    description text,
    asset_class AssetClass,
    start_date date DEFAULT CURRENT_DATE,
    symbol_base text
);

CREATE TABLE IF NOT EXISTS TradeSymbol (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    exchange_id integer REFERENCES Exchange,
    symbol_id integer REFERENCES Symbol,
    UNIQUE (exchange_id, symbol_id)
);

CREATE TABLE IF NOT EXISTS MinuteData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES TradeSymbol,
    datetime timestamp,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume integer,
    UNIQUE (trade_symbol_id, datetime)
);

CREATE TABLE IF NOT EXISTS TickData (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    trade_symbol_id integer REFERENCES Symbol,
    datetime timestamp,
    bid numeric,
    ask numeric,
    volume integer
);

INSERT INTO Exchange (name) VALUES ('TestExchange');
