CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_cc_get_main_okved(val text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

DECLARE
  okved_from INT;
  okved_to INT;
  temp TEXT;
  result TEXT;
BEGIN
  IF val IS NULL OR val = 'nan' THEN
    RETURN '';
  END IF;

  -- если в строке есть диапазон через тире
  IF position('-' IN val) > 0 THEN
    temp := split_part(val, 'коды ', 2);        -- берём часть после "коды "
    temp := left(temp, 5);                      -- первые 5 символов диапазона
    okved_from := split_part(temp, '-', 1)::INT;
    IF split_part(temp, '-', 2) = ')' THEN
      okved_to := okved_from;
    ELSE
      okved_to := split_part(temp, '-', 2)::INT;
    END IF;
  ELSE
    okved_from := val::INT;
    okved_to := okved_from;
  END IF;

  -- найти самое частое название
  SELECT okved_name INTO result
  FROM (
    SELECT s.okved_name, COUNT(*) AS freq
    FROM generate_series(okved_from, okved_to) AS n(num)
    JOIN s_grnplm_vd_hr_edp_dac.tb_ref_okved_si s ON s.okved_num::integer = n.num
    GROUP BY s.okved_name
    ORDER BY freq DESC
    LIMIT 1
  ) AS sub;

  RETURN COALESCE(result, '');
END;

$body$
EXECUTE ON ANY;
	