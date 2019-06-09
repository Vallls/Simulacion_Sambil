-- FUNCTION: public.validacionmayoredad()

-- DROP FUNCTION public.validacionmayoredad();

CREATE FUNCTION public.validacionmayoredad()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF 
AS $BODY$
DECLARE
edad integer;
BEGIN
SELECT mcadress.edad INTO edad FROM mcadress 
WHERE mcadress = new.mcadress;
	IF(edad < 21 and new.idtienda = 2) THEN
		DELETE FROM compra
		WHERE compra.factura = new.factura;
	END IF;
return new;
END
$BODY$;

ALTER FUNCTION public.validacionmayoredad()
    OWNER TO postgres;
