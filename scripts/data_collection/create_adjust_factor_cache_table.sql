CREATE TABLE IF NOT EXISTS adjust_factor_cache (
    code VARCHAR(10) NOT NULL,
    divid_operate_date DATE NOT NULL,
    fore_adjust_factor NUMERIC NOT NULL,
    source VARCHAR(32) NOT NULL DEFAULT 'baostock',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, divid_operate_date)
);

CREATE INDEX IF NOT EXISTS idx_adjust_factor_cache_date
    ON adjust_factor_cache (divid_operate_date);
