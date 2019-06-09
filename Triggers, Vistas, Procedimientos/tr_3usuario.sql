-- Trigger: tr_3usuario

-- DROP TRIGGER tr_3usuario ON public.sensor_usuario;

CREATE TRIGGER tr_3usuario
    AFTER INSERT
    ON public.sensor_usuario
    FOR EACH ROW
    EXECUTE PROCEDURE public.validaciontiempousuario();