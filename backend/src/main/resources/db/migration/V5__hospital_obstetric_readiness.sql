ALTER TABLE hospital ADD COLUMN obstetric BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE hospital SET obstetric = TRUE WHERE code IN ('H-01', 'H-03', 'H-05', 'H-07');
