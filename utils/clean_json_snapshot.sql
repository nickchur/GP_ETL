CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.clean_json_snapshot(snapshot text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$


DECLARE
   cleaned_snapshot TEXT := snapshot;
   cleaned_snapshot2 TEXT := '';
   removed_chars text := '';
   i INT;
BEGIN

   raise notice 'snapshot first: %', cleaned_snapshot;
   --1. Пробуем сразу преобразовать в JSON
   IF s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot) IS NOT NULL THEN
      raise notice 'cleaned_snapsho1t: %', cleaned_snapshot;
      RETURN s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot);
   END IF;

   cleaned_snapshot := replace(replace(replace(replace(snapshot, '\\', '\'), '\''', ''), '\\о', '\\nо'), '\\\', '\\');

   IF s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot) IS NOT NULL THEN
      raise notice 'cleaned_snapsho1t: %', cleaned_snapshot;
      RETURN s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot);
   END IF;

   select string_agg(chr(char_code), '') into removed_chars
   from (select generate_series(1000, 1039) as char_code
   union all
   select generate_series(1106, 55000)
   ) as chars;

   --raise notice 'removed_chars: %', removed_chars;

   --raise notice 'cleaned_snapshot first: %', cleaned_snapshot;

   --cleaned_snapshot := regexp_replace(snapshot, '[' || removed_chars || ']', '', 'g');

   --cleaned_snapshot := regexp_replace(snapshot, '[\x{1000}-\x{1039}\x{1106}-\x{D7FF}]', '', 'g');

   cleaned_snapshot := translate(snapshot, removed_chars, '');

   cleaned_snapshot2 := cleaned_snapshot;

   cleaned_snapshot := regexp_replace(cleaned_snapshot,'(:\s*"[^"}]+?)(,|})','\1"\2','g'); 
   cleaned_snapshot2 := regexp_replace(cleaned_snapshot2, '(":)\s*([,}])', '\1""\2', 'g'); 

   cleaned_snapshot := replace(cleaned_snapshot::text, '":",', '":"",');
   cleaned_snapshot2 := replace(cleaned_snapshot2::text, '":",', '":"",');

   --raise notice 'cleaned_snapshot: %', cleaned_snapshot;

   --raise notice 'cleaned_snapshot2: %', cleaned_snapshot2;

   if s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot) IS not NULL THEN
      RETURN s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot);
   else
      if s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot2) IS not NULL THEN
         RETURN s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot2);
      end if;
   end if;

   cleaned_snapshot := regexp_replace(cleaned_snapshot::text, '",\s*(?!")', ' ', 'g');
   cleaned_snapshot2 := regexp_replace(cleaned_snapshot2::text, '",\s*(?!")', ' ', 'g');

   if s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot) IS not NULL THEN
      RETURN s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot);
   else
      if s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot2) IS not NULL THEN
         RETURN s_grnplm_vd_hr_edp_srv_wf.try_cast2json(cleaned_snapshot2);
      end if;
   end if;

   raise notice 'cleaned_snapshot itog: %', cleaned_snapshot;
   raise notice 'cleaned_snapshot2 itog: %', cleaned_snapshot2;

   RETURN NULL;
END;


$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.clean_json_snapshot(text) IS 'Очищает и нормализует текст JSON-снимка, удаляя некорректные символы и исправляя структуру';
