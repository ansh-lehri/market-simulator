CREATE TABLE early_access_registrations (
  id          BIGSERIAL   PRIMARY KEY,
  email       TEXT        NOT NULL,
  experience  TEXT        NOT NULL,  -- 'new' | 'some' | 'active'
  market      TEXT,                   -- 'india' | 'us' | NULL
  source      TEXT,                   -- 'hero' | 'nav' | 'footer' | 'free_limit_wall'
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (email)
);
