CREATE TABLE IF NOT EXISTS extreme_pollution (
    id INT PRIMARY KEY,
    period DATE,
    subject VARCHAR(255),
    municipality VARCHAR(255),
    settlement VARCHAR(255),
    oktmo VARCHAR(20),
    water_body VARCHAR(255),
    indicator VARCHAR(255),
    hazard_class INT,
    cnt_cases INT,
    value_min DECIMAL(10,4),
    value_max DECIMAL(10,4),
    unit VARCHAR(50)
);

CREATE INDEX idx_extreme_period ON extreme_pollution (period);
CREATE INDEX idx_extreme_subject ON extreme_pollution (subject);
