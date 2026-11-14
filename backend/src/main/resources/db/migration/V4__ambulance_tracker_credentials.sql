CREATE TABLE ambulance_tracker (
  tracker_id VARCHAR(96) PRIMARY KEY,
  city VARCHAR(40) NOT NULL,
  unit_number INTEGER NOT NULL,
  token_hash VARCHAR(100) NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  last_seen_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(city, unit_number),
  FOREIGN KEY(city, unit_number) REFERENCES ambulance(city, unit_number) ON DELETE CASCADE
);
