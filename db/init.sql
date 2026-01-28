DROP DATABASE idx;

CREATE DATABASE idx;

use idx;

CREATE TABLE time_dimensions (
    `timestamp` TIMESTAMP NOT NULL,
    PRIMARY KEY (timestamp)
);

CREATE TABLE index_profiles (
    `index_code` VARCHAR(10) NOT NULL UNIQUE,
    PRIMARY KEY (index_code)
);

CREATE TABLE stock_profiles (
    `stock_code` VARCHAR(5) NOT NULL UNIQUE,
    `stock_name` VARCHAR(50),
    `remarks` VARCHAR(150),
    `delisting_date` TIMESTAMP,
    PRIMARY KEY (stock_code)
);

CREATE TABLE index_data (
    `index_profile` VARCHAR(10) NOT NULL,
    `timestamp` TIMESTAMP NOT NULL,
    `previous` DOUBLE NOT NULL,
    `highest` DOUBLE NOT NULL,
    `lowest` DOUBLE NOT NULL,
    `close` DOUBLE NOT NULL,
    `number_of_stock` DOUBLE NOT NULL,
    `change` DOUBLE NOT NULL,
    `volume` DOUBLE NOT NULL,
    `value` DOUBLE NOT NULL,
    `frequency` DOUBLE NOT NULL,
    `market_capital` DOUBLE NOT NULL,
    FOREIGN KEY (index_profile) REFERENCES index_profiles (index_code) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (timestamp) REFERENCES time_dimensions (timestamp) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (index_profile, timestamp)
);

CREATE TABLE stock_data (
    `stock_profile` VARCHAR(5) NOT NULL,
    `timestamp` TIMESTAMP NOT NULL,
    `previous` DOUBLE NOT NULL,
    `open_price` DOUBLE NOT NULL,
    `first_trade` DOUBLE NOT NULL,
    `high` DOUBLE NOT NULL,
    `low` DOUBLE NOT NULL,
    `close` DOUBLE NOT NULL,
    `change` DOUBLE NOT NULL,
    `volume` DOUBLE NOT NULL,
    `value` DOUBLE NOT NULL,
    `frequency` DOUBLE NOT NULL,
    `index_individual` DOUBLE NOT NULL,
    `offer` DOUBLE NOT NULL,
    `offer_volume` DOUBLE NOT NULL,
    `bid` DOUBLE NOT NULL,
    `bid_volume` DOUBLE NOT NULL,
    `listed_shares` DOUBLE NOT NULL,
    `tradeble_shares` DOUBLE NOT NULL,
    `weight_for_index` DOUBLE NOT NULL,
    `foreign_sell` DOUBLE NOT NULL,
    `foreign_buy` DOUBLE NOT NULL,
    `non_regular_volume` DOUBLE NOT NULL,
    `non_regular_value` DOUBLE NOT NULL,
    `non_regular_frequency` DOUBLE NOT NULL,
    `persen` DOUBLE,
    `percentage` DOUBLE,
    FOREIGN KEY (stock_profile) REFERENCES stock_profiles (stock_code) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (timestamp) REFERENCES time_dimensions (timestamp) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (stock_profile, timestamp)
);