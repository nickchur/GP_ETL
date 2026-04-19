CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_check_null(tbl_path text) 
	RETURNS TABLE(column_name text, nulls_cnt bigint)
	LANGUAGE plpgsql
	VOLATILE
as $body$


DECLARE
    dtype TEXT;
    col_name TEXT;
    sql_names TEXT := '';
    sql_counts TEXT := '';
    sql_query TEXT;

BEGIN
    FOR col_name, dtype IN SELECT c.column_name::text, c.data_type::text FROM information_schema.columns c
                            WHERE c.table_schema = split_part(tbl_path, '.', 1)
                            AND c.table_name = split_part(tbl_path, '.', 2)
                            ORDER BY c.ordinal_position
    LOOP
        sql_names := sql_names || quote_literal(col_name) || ', ';

        IF dtype IN ('text', 'character_varying', 'character', 'varchar')
        THEN
            sql_counts := sql_counts || 'count(1) FILTER(WHERE NULLIF(NULLIF(' || quote_ident(col_name) || ', ''''), '' '') IS NULL), ';
        ELSE
            sql_counts := sql_counts || 'count(1) FILTER(WHERE ' || quote_ident(col_name) || ' IS NULL), ';
        END IF;
    END LOOP;

    sql_names := left(sql_names, length(sql_names) - 2);
    sql_counts := left(sql_counts, length(sql_counts) - 2);

    sql_query := 'SELECT unnest(ARRAY[' || sql_names || ']::text[]), ' || ' unnest(ARRAY[' || sql_counts || ']::bigint[]) ' || 'FROM ' || tbl_path;
    RETURN QUERY EXECUTE sql_query;

END

$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_check_null(text) IS 'Возвращает количество NULL-значений по каждой колонке таблицы (текстовые поля учитывают пустые строки)';
