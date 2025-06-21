CREATE TABLE IF NOT EXISTS pdk (
    ingredient VARCHAR(255),
    ingredient_npa VARCHAR(255),
    hazard_class VARCHAR(10),  -- изменили тип с INT на VARCHAR
    value DECIMAL(10,4),
    lpv VARCHAR(100),
    formula VARCHAR(100),
    n_cas VARCHAR(50),
    npa VARCHAR(255),
    date_start DATE,
    date_end DATE
);

CREATE INDEX idx_pdk_ingredient ON pdk (ingredient);
CREATE INDEX idx_pdk_date_start ON pdk (date_start);
