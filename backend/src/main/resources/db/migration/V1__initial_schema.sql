CREATE TABLE app_user (
  id BIGSERIAL PRIMARY KEY,
  username VARCHAR(80) NOT NULL UNIQUE,
  password_hash VARCHAR(100) NOT NULL,
  role VARCHAR(32) NOT NULL CHECK (role IN ('HOSPITAL_ADMIN','PARAMEDIC')),
  enabled BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE hospital (
  id BIGSERIAL PRIMARY KEY,
  city VARCHAR(40) NOT NULL,
  code VARCHAR(24) NOT NULL,
  name VARCHAR(180) NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  icu_beds INTEGER NOT NULL CHECK (icu_beds >= 0),
  trauma BOOLEAN NOT NULL,
  cardiac BOOLEAN NOT NULL,
  stroke BOOLEAN NOT NULL,
  burns BOOLEAN NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(city, code)
);

CREATE TABLE ambulance (
  id BIGSERIAL PRIMARY KEY,
  city VARCHAR(40) NOT NULL,
  unit_number INTEGER NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  status VARCHAR(24) NOT NULL CHECK (status IN ('AVAILABLE','DISPATCHED')),
  battery INTEGER NOT NULL CHECK (battery BETWEEN 0 AND 100),
  heading INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(city, unit_number)
);

CREATE TABLE incident (
  id UUID PRIMARY KEY,
  city VARCHAR(40) NOT NULL,
  incident_number VARCHAR(40) NOT NULL UNIQUE,
  type VARCHAR(100) NOT NULL,
  triage VARCHAR(16) NOT NULL CHECK (triage IN ('Critical','Urgent','Standard')),
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  notes VARCHAR(2000),
  status VARCHAR(24) NOT NULL CHECK (status IN ('DISPATCHED','TRANSPORTING','COMPLETED')),
  ambulance_unit INTEGER NOT NULL,
  hospital_code VARCHAR(24) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX incident_city_created_idx ON incident(city, created_at DESC);
CREATE INDEX ambulance_city_status_idx ON ambulance(city, status);
