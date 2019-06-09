-- Trigger: tr_3compra

-- DROP TRIGGER tr_3compra ON public.compra;

CREATE TRIGGER tr_3compra
    AFTER INSERT
    ON public.compra
    FOR EACH ROW
    EXECUTE PROCEDURE public.validaciontiempocompra();