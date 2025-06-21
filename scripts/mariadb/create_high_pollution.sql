CREATE TABLE IF NOT EXISTS high_pollution (
    id INT PRIMARY KEY,
    period DATE,
    okato VARCHAR(20),          -- поменяли местами с subject
    subject VARCHAR(255),
    river_basin VARCHAR(255),
    indicator VARCHAR(255),
    hazard_class VARCHAR(10),   -- изменили тип с INT на VARCHAR
    cnt_cases INT,
    value_min DECIMAL(10,4),
    value_max DECIMAL(10,4),
    unit VARCHAR(50)
);

CREATE INDEX idx_high_period ON high_pollution (period);
CREATE INDEX idx_high_subject ON high_pollution (subject);
