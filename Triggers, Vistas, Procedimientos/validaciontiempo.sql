-- FUNCTION: public.validaciontiempo()

-- DROP FUNCTION public.validaciontiempo();

CREATE FUNCTION public.validaciontiempo()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF 
AS $BODY$
DECLARE
Tiempo Varchar;
Hora Varchar;
horaint integer;
BEGIN
SELECT split_part(new.fecha_hora, ' ', 2) into Tiempo;
SELECT split_part(Tiempo,':',1) into Hora;
SELECT CAST (Hora AS INTEGER) into horaint;
IF(horaint > 20) THEN
	DELETE FROM sensor_mcadress
	WHERE sensor_mcadress.fecha_hora = new.fecha_hora;
END IF;
return new;
END
$BODY$;

ALTER FUNCTION public.validaciontiempo()
    OWNER TO postgres;
