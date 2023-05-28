-- IMPORTANT NOTE
-- This script assumes an empty database with an empty schema 'public'
--
-- PostgreSQL database dump
--

-- Dumped from database version 13.9 (Debian 13.9-1.pgdg110+1)
-- Dumped by pg_dump version 13.9 (Ubuntu 13.9-1.pgdg20.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: assetclass; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.assetclass AS ENUM (
    'futures',
    'etfs',
    'forex',
    'stocks',
    'sp_500'
);


--
-- Name: contracttype; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.contracttype AS ENUM (
    'continuous',
    'expiry'
);


--
-- Name: frequency; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.frequency AS ENUM (
    'minute',
    'daily',
    'tick'
);


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: ccxt_bid_ask_futures_raw; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.ccxt_bid_ask_futures_raw (
    id bigint NOT NULL,
    "timestamp" bigint NOT NULL,
    bid_size numeric,
    bid_price numeric,
    ask_size numeric,
    ask_price numeric,
    currency_pair character varying(255) NOT NULL,
    exchange_id character varying(255) NOT NULL,
    level integer NOT NULL,
    end_download_timestamp timestamp with time zone,
    knowledge_timestamp timestamp with time zone
);

--
-- Name: ccxt_bid_ask_futures_raw_timestamp_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS ccxt_bid_ask_futures_raw_timestamp_index ON public.ccxt_bid_ask_futures_raw(timestamp);

--
-- Name: ccxt_bid_ask_futures_raw_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ccxt_bid_ask_futures_raw_id_seq
    AS bigint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ccxt_bid_ask_futures_raw_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ccxt_bid_ask_futures_raw_id_seq OWNED BY public.ccxt_bid_ask_futures_raw.id;


--
-- Name: ccxt_bid_ask_futures_resampled_1min; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.ccxt_bid_ask_futures_resampled_1min (
    id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    bid_size numeric,
    bid_price numeric,
    ask_size numeric,
    ask_price numeric,
    currency_pair character varying(255) NOT NULL,
    exchange_id character varying(255) NOT NULL,
    level integer NOT NULL,
    end_download_timestamp timestamp with time zone,
    knowledge_timestamp timestamp with time zone
);


--
-- Name: ccxt_bid_ask_futures_resampled_1min_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ccxt_bid_ask_futures_resampled_1min_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ccxt_bid_ask_futures_resampled_1min_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ccxt_bid_ask_futures_resampled_1min_id_seq OWNED BY public.ccxt_bid_ask_futures_resampled_1min.id;


--
-- Name: ccxt_bid_ask_raw; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.ccxt_bid_ask_raw (
    id bigint NOT NULL,
    "timestamp" bigint NOT NULL,
    bid_size numeric,
    bid_price numeric,
    ask_size numeric,
    ask_price numeric,
    currency_pair character varying(255) NOT NULL,
    exchange_id character varying(255) NOT NULL,
    level integer NOT NULL,
    end_download_timestamp timestamp with time zone,
    knowledge_timestamp timestamp with time zone
);

--
-- Name: ccxt_bid_ask_raw_timestamp_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS ccxt_bid_ask_raw_timestamp_index ON public.ccxt_bid_ask_raw(timestamp);


--
-- Name: ccxt_bid_ask_raw_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ccxt_bid_ask_raw_id_seq
    AS bigint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ccxt_bid_ask_raw_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ccxt_bid_ask_raw_id_seq OWNED BY public.ccxt_bid_ask_raw.id;


--
-- Name: ccxt_bid_ask_resampled_1min; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.ccxt_bid_ask_resampled_1min (
    id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    bid_size numeric,
    bid_price numeric,
    ask_size numeric,
    ask_price numeric,
    currency_pair character varying(255) NOT NULL,
    exchange_id character varying(255) NOT NULL,
    level integer NOT NULL,
    end_download_timestamp timestamp with time zone,
    knowledge_timestamp timestamp with time zone
);


--
-- Name: ccxt_bid_ask_resampled_1min_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ccxt_bid_ask_resampled_1min_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ccxt_bid_ask_resampled_1min_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ccxt_bid_ask_resampled_1min_id_seq OWNED BY public.ccxt_bid_ask_resampled_1min.id;


--
-- Name: ccxt_ohlcv; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.ccxt_ohlcv (
    id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    currency_pair character varying(255) NOT NULL,
    exchange_id character varying(255) NOT NULL,
    end_download_timestamp timestamp with time zone,
    knowledge_timestamp timestamp with time zone
);


--
-- Name: ccxt_ohlcv_futures; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.ccxt_ohlcv_futures (
    id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    currency_pair character varying(255) NOT NULL,
    exchange_id character varying(255) NOT NULL,
    end_download_timestamp timestamp with time zone,
    knowledge_timestamp timestamp with time zone
);


--
-- Name: ccxt_ohlcv_futures_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ccxt_ohlcv_futures_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ccxt_ohlcv_futures_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ccxt_ohlcv_futures_id_seq OWNED BY public.ccxt_ohlcv_futures.id;


--
-- Name: ccxt_ohlcv_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ccxt_ohlcv_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ccxt_ohlcv_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ccxt_ohlcv_id_seq OWNED BY public.ccxt_ohlcv.id;


--
-- Name: currency_pair; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.currency_pair (
    currency_pair_id integer NOT NULL,
    currency_pair character varying(255) NOT NULL
);


--
-- Name: currency_pair_currency_pair_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.currency_pair_currency_pair_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: currency_pair_currency_pair_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.currency_pair_currency_pair_id_seq OWNED BY public.currency_pair.currency_pair_id;


--
-- Name: serial; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.serial
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: exchange; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.exchange (
    id integer DEFAULT nextval('public.serial'::regclass) NOT NULL,
    name text
);


--
-- Name: exchange_name; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.exchange_name (
    exchange_id integer NOT NULL,
    exchange_name character varying(255) NOT NULL
);


--
-- Name: exchange_name_exchange_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.exchange_name_exchange_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: exchange_name_exchange_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.exchange_name_exchange_id_seq OWNED BY public.exchange_name.exchange_id;


--
-- Name: ib_daily_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.ib_daily_data (
    id integer DEFAULT nextval('public.serial'::regclass) NOT NULL,
    trade_symbol_id integer,
    date date,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume bigint,
    average numeric,
    barcount integer
);


--
-- Name: ib_minute_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.ib_minute_data (
    id integer DEFAULT nextval('public.serial'::regclass) NOT NULL,
    trade_symbol_id integer,
    datetime timestamp with time zone,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume bigint,
    average numeric,
    barcount integer
);


--
-- Name: ib_tick_bid_ask_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.ib_tick_bid_ask_data (
    id integer DEFAULT nextval('public.serial'::regclass) NOT NULL,
    trade_symbol_id integer,
    datetime timestamp without time zone,
    bid numeric,
    ask numeric,
    volume bigint
);


--
-- Name: ib_tick_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.ib_tick_data (
    id integer DEFAULT nextval('public.serial'::regclass) NOT NULL,
    trade_symbol_id integer,
    datetime timestamp without time zone,
    price numeric,
    size bigint
);


--
-- Name: kibot_daily_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.kibot_daily_data (
    id integer DEFAULT nextval('public.serial'::regclass) NOT NULL,
    trade_symbol_id integer,
    date date,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume bigint
);


--
-- Name: kibot_minute_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.kibot_minute_data (
    id integer DEFAULT nextval('public.serial'::regclass) NOT NULL,
    trade_symbol_id integer,
    datetime timestamp without time zone,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume bigint
);


--
-- Name: kibot_tick_bid_ask_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.kibot_tick_bid_ask_data (
    id integer DEFAULT nextval('public.serial'::regclass) NOT NULL,
    trade_symbol_id integer,
    datetime timestamp without time zone,
    bid numeric,
    ask numeric,
    volume bigint
);


--
-- Name: kibot_tick_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.kibot_tick_data (
    id integer DEFAULT nextval('public.serial'::regclass) NOT NULL,
    trade_symbol_id integer,
    datetime timestamp without time zone,
    price numeric,
    size bigint
);


--
-- Name: symbol; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.symbol (
    id integer DEFAULT nextval('public.serial'::regclass) NOT NULL,
    code text,
    description text,
    asset_class public.assetclass,
    start_date date DEFAULT CURRENT_DATE,
    symbol_base text
);


--
-- Name: trade_symbol; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.trade_symbol (
    id integer DEFAULT nextval('public.serial'::regclass) NOT NULL,
    exchange_id integer,
    symbol_id integer
);


--
-- Name: ccxt_bid_ask_futures_raw id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_bid_ask_futures_raw ALTER COLUMN id SET DEFAULT nextval('public.ccxt_bid_ask_futures_raw_id_seq'::regclass);


--
-- Name: ccxt_bid_ask_futures_resampled_1min id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_bid_ask_futures_resampled_1min ALTER COLUMN id SET DEFAULT nextval('public.ccxt_bid_ask_futures_resampled_1min_id_seq'::regclass);


--
-- Name: ccxt_bid_ask_raw id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_bid_ask_raw ALTER COLUMN id SET DEFAULT nextval('public.ccxt_bid_ask_raw_id_seq'::regclass);


--
-- Name: ccxt_bid_ask_resampled_1min id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_bid_ask_resampled_1min ALTER COLUMN id SET DEFAULT nextval('public.ccxt_bid_ask_resampled_1min_id_seq'::regclass);


--
-- Name: ccxt_ohlcv id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_ohlcv ALTER COLUMN id SET DEFAULT nextval('public.ccxt_ohlcv_id_seq'::regclass);


--
-- Name: ccxt_ohlcv_futures id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_ohlcv_futures ALTER COLUMN id SET DEFAULT nextval('public.ccxt_ohlcv_futures_id_seq'::regclass);


--
-- Name: currency_pair currency_pair_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.currency_pair ALTER COLUMN currency_pair_id SET DEFAULT nextval('public.currency_pair_currency_pair_id_seq'::regclass);


--
-- Name: exchange_name exchange_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange_name ALTER COLUMN exchange_id SET DEFAULT nextval('public.exchange_name_exchange_id_seq'::regclass);


--
-- Name: ccxt_bid_ask_futures_raw ccxt_bid_ask_futures_raw_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_bid_ask_futures_raw
    ADD CONSTRAINT ccxt_bid_ask_futures_raw_pkey PRIMARY KEY (id);


--
-- Name: ccxt_bid_ask_futures_resampled_1min ccxt_bid_ask_futures_resampled_1min_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_bid_ask_futures_resampled_1min
    ADD CONSTRAINT ccxt_bid_ask_futures_resampled_1min_pkey PRIMARY KEY (id);


--
-- Name: ccxt_bid_ask_raw ccxt_bid_ask_raw_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_bid_ask_raw
    ADD CONSTRAINT ccxt_bid_ask_raw_pkey PRIMARY KEY (id);


--
-- Name: ccxt_bid_ask_resampled_1min ccxt_bid_ask_resampled_1min_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_bid_ask_resampled_1min
    ADD CONSTRAINT ccxt_bid_ask_resampled_1min_pkey PRIMARY KEY (id);


--
-- Name: ccxt_ohlcv_futures ccxt_ohlcv_futures_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_ohlcv_futures
    ADD CONSTRAINT ccxt_ohlcv_futures_pkey PRIMARY KEY (id);


--
-- Name: ccxt_ohlcv_futures ccxt_ohlcv_futures_timestamp_exchange_id_currency_pair_open_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_ohlcv_futures
    ADD CONSTRAINT ccxt_ohlcv_futures_timestamp_exchange_id_currency_pair_open_key UNIQUE ("timestamp", exchange_id, currency_pair, open, high, low, close, volume);


--
-- Name: ccxt_ohlcv ccxt_ohlcv_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_ohlcv
    ADD CONSTRAINT ccxt_ohlcv_pkey PRIMARY KEY (id);


--
-- Name: ccxt_ohlcv ccxt_ohlcv_timestamp_exchange_id_currency_pair_open_high_lo_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ccxt_ohlcv
    ADD CONSTRAINT ccxt_ohlcv_timestamp_exchange_id_currency_pair_open_high_lo_key UNIQUE ("timestamp", exchange_id, currency_pair, open, high, low, close, volume);


--
-- Name: currency_pair currency_pair_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.currency_pair
    ADD CONSTRAINT currency_pair_pkey PRIMARY KEY (currency_pair_id);


--
-- Name: exchange exchange_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange
    ADD CONSTRAINT exchange_name_key UNIQUE (name);


--
-- Name: exchange_name exchange_name_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange_name
    ADD CONSTRAINT exchange_name_pkey PRIMARY KEY (exchange_id);


--
-- Name: exchange exchange_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange
    ADD CONSTRAINT exchange_pkey PRIMARY KEY (id);


--
-- Name: ib_daily_data ib_daily_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ib_daily_data
    ADD CONSTRAINT ib_daily_data_pkey PRIMARY KEY (id);


--
-- Name: ib_daily_data ib_daily_data_trade_symbol_id_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ib_daily_data
    ADD CONSTRAINT ib_daily_data_trade_symbol_id_date_key UNIQUE (trade_symbol_id, date);


--
-- Name: ib_minute_data ib_minute_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ib_minute_data
    ADD CONSTRAINT ib_minute_data_pkey PRIMARY KEY (id);


--
-- Name: ib_minute_data ib_minute_data_trade_symbol_id_datetime_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ib_minute_data
    ADD CONSTRAINT ib_minute_data_trade_symbol_id_datetime_key UNIQUE (trade_symbol_id, datetime);


--
-- Name: ib_tick_bid_ask_data ib_tick_bid_ask_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ib_tick_bid_ask_data
    ADD CONSTRAINT ib_tick_bid_ask_data_pkey PRIMARY KEY (id);


--
-- Name: ib_tick_data ib_tick_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ib_tick_data
    ADD CONSTRAINT ib_tick_data_pkey PRIMARY KEY (id);


--
-- Name: kibot_daily_data kibot_daily_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kibot_daily_data
    ADD CONSTRAINT kibot_daily_data_pkey PRIMARY KEY (id);


--
-- Name: kibot_daily_data kibot_daily_data_trade_symbol_id_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kibot_daily_data
    ADD CONSTRAINT kibot_daily_data_trade_symbol_id_date_key UNIQUE (trade_symbol_id, date);


--
-- Name: kibot_minute_data kibot_minute_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kibot_minute_data
    ADD CONSTRAINT kibot_minute_data_pkey PRIMARY KEY (id);


--
-- Name: kibot_minute_data kibot_minute_data_trade_symbol_id_datetime_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kibot_minute_data
    ADD CONSTRAINT kibot_minute_data_trade_symbol_id_datetime_key UNIQUE (trade_symbol_id, datetime);


--
-- Name: kibot_tick_bid_ask_data kibot_tick_bid_ask_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kibot_tick_bid_ask_data
    ADD CONSTRAINT kibot_tick_bid_ask_data_pkey PRIMARY KEY (id);


--
-- Name: kibot_tick_data kibot_tick_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kibot_tick_data
    ADD CONSTRAINT kibot_tick_data_pkey PRIMARY KEY (id);


--
-- Name: symbol symbol_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.symbol
    ADD CONSTRAINT symbol_code_key UNIQUE (code);


--
-- Name: symbol symbol_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.symbol
    ADD CONSTRAINT symbol_pkey PRIMARY KEY (id);


--
-- Name: trade_symbol trade_symbol_exchange_id_symbol_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trade_symbol
    ADD CONSTRAINT trade_symbol_exchange_id_symbol_id_key UNIQUE (exchange_id, symbol_id);


--
-- Name: trade_symbol trade_symbol_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trade_symbol
    ADD CONSTRAINT trade_symbol_pkey PRIMARY KEY (id);


--
-- Name: ib_daily_data ib_daily_data_trade_symbol_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ib_daily_data
    ADD CONSTRAINT ib_daily_data_trade_symbol_id_fkey FOREIGN KEY (trade_symbol_id) REFERENCES public.trade_symbol(id);


--
-- Name: ib_minute_data ib_minute_data_trade_symbol_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ib_minute_data
    ADD CONSTRAINT ib_minute_data_trade_symbol_id_fkey FOREIGN KEY (trade_symbol_id) REFERENCES public.trade_symbol(id);


--
-- Name: ib_tick_bid_ask_data ib_tick_bid_ask_data_trade_symbol_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ib_tick_bid_ask_data
    ADD CONSTRAINT ib_tick_bid_ask_data_trade_symbol_id_fkey FOREIGN KEY (trade_symbol_id) REFERENCES public.trade_symbol(id);


--
-- Name: ib_tick_data ib_tick_data_trade_symbol_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ib_tick_data
    ADD CONSTRAINT ib_tick_data_trade_symbol_id_fkey FOREIGN KEY (trade_symbol_id) REFERENCES public.trade_symbol(id);


--
-- Name: kibot_daily_data kibot_daily_data_trade_symbol_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kibot_daily_data
    ADD CONSTRAINT kibot_daily_data_trade_symbol_id_fkey FOREIGN KEY (trade_symbol_id) REFERENCES public.trade_symbol(id);


--
-- Name: kibot_minute_data kibot_minute_data_trade_symbol_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kibot_minute_data
    ADD CONSTRAINT kibot_minute_data_trade_symbol_id_fkey FOREIGN KEY (trade_symbol_id) REFERENCES public.trade_symbol(id);


--
-- Name: kibot_tick_bid_ask_data kibot_tick_bid_ask_data_trade_symbol_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kibot_tick_bid_ask_data
    ADD CONSTRAINT kibot_tick_bid_ask_data_trade_symbol_id_fkey FOREIGN KEY (trade_symbol_id) REFERENCES public.trade_symbol(id);


--
-- Name: kibot_tick_data kibot_tick_data_trade_symbol_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kibot_tick_data
    ADD CONSTRAINT kibot_tick_data_trade_symbol_id_fkey FOREIGN KEY (trade_symbol_id) REFERENCES public.trade_symbol(id);


--
-- Name: trade_symbol trade_symbol_exchange_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trade_symbol
    ADD CONSTRAINT trade_symbol_exchange_id_fkey FOREIGN KEY (exchange_id) REFERENCES public.exchange(id);


--
-- Name: trade_symbol trade_symbol_symbol_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trade_symbol
    ADD CONSTRAINT trade_symbol_symbol_id_fkey FOREIGN KEY (symbol_id) REFERENCES public.symbol(id);

--
-- PostgreSQL database dump complete
--
