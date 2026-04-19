CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.preproccess_text(inp text, trimed integer DEFAULT 1) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

 
       begin
             if lower(inp) similar to '%([a-z]|[а-я]|\d)+%' then
                    if trimed = 0 then
                           return inp;
                    end if;
                    return trim(trim(inp), e'\t');
             end if;
             return null;
       end;
 

$body$
EXECUTE ON ANY;
	