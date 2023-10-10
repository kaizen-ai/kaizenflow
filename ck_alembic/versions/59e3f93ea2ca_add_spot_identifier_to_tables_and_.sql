CREATE TABLE IF NOT EXISTS public.ccxt_trades_spot_downloaded_all (
    id BIGSERIAL PRIMARY KEY,
    "timestamp" bigint NOT NULL,
    side character varying(255) CHECK (side IN ('buy', 'sell')),
    price numeric,
    amount numeric,
    currency_pair character varying(255) NOT NULL,
    exchange_id character varying(255) NOT NULL,
    end_download_timestamp timestamp with time zone,
    knowledge_timestamp timestamp with time zone
);

CREATE INDEX IF NOT EXISTS ccxt_trades_spot_downloaded_all_timestamp_index
ON public.ccxt_trades_spot_downloaded_all(timestamp);

CREATE TABLE IF NOT EXISTS public.ccxt_trades_futures_downloaded_all (
    id BIGSERIAL PRIMARY KEY,
    "timestamp" bigint NOT NULL,
    side character varying(255) CHECK (side IN ('buy', 'sell')),
    price numeric,
    amount numeric,
    currency_pair character varying(255) NOT NULL,
    exchange_id character varying(255) NOT NULL,
    end_download_timestamp timestamp with time zone,
    knowledge_timestamp timestamp with time zone
);

CREATE INDEX IF NOT EXISTS ccxt_trades_futures_downloaded_all_timestamp_index
ON public.ccxt_trades_futures_downloaded_all(timestamp);

CREATE TABLE IF NOT EXISTS public.ccxt_bid_ask_spot_raw(
    id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    bid_size NUMERIC,
    bid_price NUMERIC,
    ask_size NUMERIC,
    ask_price NUMERIC,
    currency_pair VARCHAR(255) NOT NULL,
    exchange_id VARCHAR(255) NOT NULL,
    level INTEGER NOT NULL,
    end_download_timestamp TIMESTAMP WITH TIME ZONE,
    knowledge_timestamp TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS ccxt_bid_ask_spot_raw_timestamp_index
ON public.ccxt_bid_ask_spot_raw(timestamp);

CREATE TABLE IF NOT EXISTS public.ccxt_bid_ask_spot_resampled_1min(
    id SERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    bid_size NUMERIC,
    bid_price NUMERIC,
    ask_size NUMERIC,
    ask_price NUMERIC,
    currency_pair VARCHAR(255) NOT NULL,
    exchange_id VARCHAR(255) NOT NULL,
    level INTEGER NOT NULL,
    end_download_timestamp TIMESTAMP WITH TIME ZONE,
    knowledge_timestamp TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS ccxt_bid_ask_spot_resampled_1min_timestamp_index
ON public.ccxt_bid_ask_spot_resampled_1min(timestamp);

CREATE TABLE IF NOT EXISTS ccxt_ohlcv_spot(
    id SERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    currency_pair VARCHAR(255) NOT NULL,
    exchange_id VARCHAR(255) NOT NULL,
    end_download_timestamp TIMESTAMP WITH TIME ZONE,
    knowledge_timestamp TIMESTAMP WITH TIME ZONE,
    UNIQUE(timestamp, exchange_id,
    currency_pair, open, high, low, close, volume)
);

CREATE INDEX IF NOT EXISTS ccxt_ohlcv_spot_timestamp_index
ON public.ccxt_ohlcv_spot(timestamp);

-- The index for OHLCV futures was not created in previous revision.
CREATE INDEX IF NOT EXISTS ccxt_ohlcv_futures_timestamp_index
ON public.ccxt_ohlcv_futures(timestamp);


