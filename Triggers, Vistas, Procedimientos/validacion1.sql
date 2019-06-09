-- FUNCTION: public.validacion1()

-- DROP FUNCTION public.validacion1();

CREATE FUNCTION public.validacion1()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF 
AS $BODY$
DECLARE
cedula INTEGER;

BEGIN 
select mcadress.cedula into cedula from mcadress 
WHERE mcadress = new.mcadress;

IF(new.cedula <> cedula) THEN
	DELETE FROM compra 
	WHERE compra.factura = new.factura;
END IF;
return new;
END
$BODY$;

ALTER FUNCTION public.validacion1()
    OWNER TO postgres;
