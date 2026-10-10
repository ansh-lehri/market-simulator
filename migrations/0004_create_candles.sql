-- +goose Up
CREATE TABLE candles (
    market      TEXT        NOT NULL,              -- broker-side market label ('IN', ...); exchange InstrumentID carries none
    instrument  TEXT        NOT NULL,              -- contract.InstrumentID (opaque)
    timestamp   TIMESTAMPTZ NOT NULL,              -- bucket start, stored UTC (DESIGN §1.3)
    open        BIGINT      NOT NULL,              -- integer ticks (never float)
    high        BIGINT      NOT NULL,
    low         BIGINT      NOT NULL,
    close       BIGINT      NOT NULL,
    volume      BIGINT      NOT NULL,              -- integer qty; 0 is valid (a flat zero-trade minute)

    CONSTRAINT pk_candles PRIMARY KEY (market, instrument, timestamp)
    -- PK is the only index; it also serves the forward keyset range scan
    -- (market, instrument, timestamp ASC) that RangeRead1m rides. No
    -- secondary index (DESIGN §1.1).
);

-- +goose Down
DROP TABLE candles;
