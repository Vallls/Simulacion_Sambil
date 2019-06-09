-- FUNCTION: public.validaciontiempocompra()

-- DROP FUNCTION public.validaciontiempocompra();

CREATE FUNCTION public.validaciontiempocompra()
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
	DELETE FROM compra
	WHERE compra.fecha_hora = new.fecha_hora;
END IF;
return new;
END
$BODY$;

ALTER FUNCTION public.validaciontiempocompra()
    OWNER TO postgres;
