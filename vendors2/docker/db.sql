CREATE TYPE assetClass AS ENUM ('Futures', 'etfs', 'forex', 'stocks', 'sp_500');
CREATE TYPE frequency AS ENUM ('T', 'D', 'tick');
CREATE TYPE contractType AS ENUM ('continuous', 'expiry');
CREATE SEQUENCE serial START 1;

CREATE TABLE IF NOT EXISTS tickers (
    id integer PRIMARY KEY DEFAULT nextval('serial'),
    symbol text,
    date date,
    time time,
    assetClass assetClass,
    frequency frequency,
    contractType contractType,
    unadjusted boolean,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    vol numeric
)
