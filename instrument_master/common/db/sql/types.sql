/* TODO: Futures -> futures */
CREATE TYPE AssetClass AS ENUM ('Futures', 'etfs', 'forex', 'stocks', 'sp_500');
/* TODO: T -> minute, D -> daily */
CREATE TYPE Frequency AS ENUM ('T', 'D', 'tick');
CREATE TYPE ContractType AS ENUM ('continuous', 'expiry');
CREATE SEQUENCE serial START 1;
