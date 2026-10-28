-- Each guest identity can start at most FREE_SESSION_LIMIT practice sessions before
-- being asked to register. The consume handler increments `used` atomically using
-- a conditional upsert so that the limit is enforced without a separate read.
CREATE TABLE guest_free_sessions (
  guest_id   TEXT        PRIMARY KEY,
  used       SMALLINT    NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
