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
