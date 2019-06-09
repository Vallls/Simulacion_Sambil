-- Trigger: tr_2

-- DROP TRIGGER tr_2 ON public.compra;

CREATE TRIGGER tr_2
    AFTER INSERT
    ON public.compra
    FOR EACH ROW
    EXECUTE PROCEDURE public.validacionmayoredad();