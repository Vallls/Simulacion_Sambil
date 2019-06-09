-- Trigger: tr_1

-- DROP TRIGGER tr_1 ON public.compra;

CREATE TRIGGER tr_1
    AFTER INSERT
    ON public.compra
    FOR EACH ROW
    EXECUTE PROCEDURE public.validacion1();