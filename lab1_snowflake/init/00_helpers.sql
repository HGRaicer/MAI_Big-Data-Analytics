
CREATE OR REPLACE FUNCTION blank_to_null(value TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(BTRIM(value), '')
$$;

CREATE OR REPLACE FUNCTION key_part(value TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT COALESCE(LOWER(blank_to_null(value)), '<null>')
$$;

CREATE OR REPLACE FUNCTION to_int(value TEXT)
RETURNS INTEGER
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN blank_to_null(value) IS NULL THEN NULL
        ELSE blank_to_null(value)::INTEGER
    END
$$;

CREATE OR REPLACE FUNCTION to_num(value TEXT)
RETURNS NUMERIC
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN blank_to_null(value) IS NULL THEN NULL
        ELSE blank_to_null(value)::NUMERIC
    END
$$;

CREATE OR REPLACE FUNCTION to_mdy_date(value TEXT)
RETURNS DATE
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN blank_to_null(value) IS NULL THEN NULL
        ELSE TO_DATE(blank_to_null(value), 'MM/DD/YYYY')
    END
$$;
