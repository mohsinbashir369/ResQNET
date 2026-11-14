ALTER TABLE ambulance
  ADD COLUMN gps_source VARCHAR(12) NOT NULL DEFAULT 'SEED'
    CHECK (gps_source IN ('SEED', 'DEMO', 'LIVE'));

ALTER TABLE ambulance
  ADD COLUMN home_latitude DOUBLE PRECISION,
  ADD COLUMN home_longitude DOUBLE PRECISION;

UPDATE ambulance SET home_latitude = latitude, home_longitude = longitude;

ALTER TABLE ambulance
  ALTER COLUMN home_latitude SET NOT NULL,
  ALTER COLUMN home_longitude SET NOT NULL;

ALTER TABLE hospital
  ADD COLUMN total_beds INTEGER NOT NULL DEFAULT 100 CHECK (total_beds >= icu_beds);

UPDATE hospital SET total_beds = CASE code
  WHEN 'H-01' THEN 1000 WHEN 'H-02' THEN 600 WHEN 'H-03' THEN 500
  WHEN 'H-04' THEN 300 WHEN 'H-05' THEN 400 WHEN 'H-06' THEN 1300
  WHEN 'H-07' THEN 500 WHEN 'H-08' THEN 350 WHEN 'H-09' THEN 250
  WHEN 'H-10' THEN 400 WHEN 'H-11' THEN 450 WHEN 'H-12' THEN 320
  ELSE 100 END;

ALTER TABLE ambulance DROP CONSTRAINT ambulance_status_check;
ALTER TABLE ambulance ADD CONSTRAINT ambulance_status_check
  CHECK (status IN ('AVAILABLE', 'DISPATCHED', 'RETURNING'));
