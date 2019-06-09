-- Trigger: tr_3

-- DROP TRIGGER tr_3 ON public.sensor_mcadress;

CREATE TRIGGER tr_3
    AFTER INSERT
    ON public.sensor_mcadress
    FOR EACH ROW
    EXECUTE PROCEDURE public.validaciontiempo();