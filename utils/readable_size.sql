CREATE OR REPLACE FUNCTION readable_size(
    size_bytes  NUMERIC,
    base        INTEGER DEFAULT 1024,
    unit_system TEXT    DEFAULT 'auto'   -- 'auto' | 'binary' | 'decimal'
)
RETURNS TEXT AS $$
DECLARE
    units      TEXT[];
    i          INTEGER;
    size_value NUMERIC;
    sign       TEXT := '';
    abs_size   NUMERIC;
BEGIN
    -- Fix #1: validate base
    IF base < 2 THEN
        RAISE EXCEPTION 'base must be >= 2, got %', base;
    END IF;

    -- Fix #4: decouple unit language from base value
    IF unit_system = 'auto' THEN
        unit_system := CASE WHEN base = 1024 THEN 'binary' ELSE 'decimal' END;
    END IF;

    IF unit_system = 'binary' THEN
        units := ARRAY['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    ELSIF unit_system = 'decimal' THEN
        units := ARRAY['ед', 'тыс', 'млн', 'млрд', 'трлн', 'квдрлн'];  -- Fix #3
    ELSE
        RAISE EXCEPTION 'unit_system must be ''auto'', ''binary'', or ''decimal'', got %', unit_system;
    END IF;

    -- Fix #5: NULL → NULL instead of '0 B'
    IF size_bytes IS NULL THEN
        RETURN NULL;
    END IF;

    IF size_bytes = 0 THEN
        RETURN '0 ' || units[1];
    END IF;

    IF size_bytes < 0 THEN
        sign     := '-';
        abs_size := abs(size_bytes);
    ELSE
        abs_size := size_bytes;
    END IF;

    -- Fix #6: collapsed bounds clamping; explicit cast for log base
    i := floor(log(base::NUMERIC, abs_size))::INTEGER;
    i := GREATEST(0, LEAST(i, array_length(units, 1) - 1));

    size_value := round(abs_size / (base::NUMERIC ^ i), 2);

    -- Fix #2: strip trailing zeros (1.00 → 1, 1.50 → 1.5)
    RETURN sign || rtrim(rtrim(size_value::TEXT, '0'), '.') || ' ' || units[i + 1];
END;
$$ LANGUAGE plpgsql IMMUTABLE;
