-- View: public.veces_utilizada_entradas

-- DROP VIEW public.veces_utilizada_entradas;

CREATE OR REPLACE VIEW public.veces_utilizada_entradas AS
 SELECT entrada.id,
        CASE
            WHEN entrada.id = 1 THEN (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 1
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 1
              GROUP BY sensor_mcadress.idsensor)) + (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 2
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 2
              GROUP BY sensor_mcadress.idsensor))
            WHEN entrada.id = 2 THEN (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 3
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 3
              GROUP BY sensor_mcadress.idsensor)) + (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 4
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 4
              GROUP BY sensor_mcadress.idsensor))
            WHEN entrada.id = 3 THEN (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 5
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 5
              GROUP BY sensor_mcadress.idsensor)) + (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 6
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 6
              GROUP BY sensor_mcadress.idsensor))
            ELSE NULL::bigint
        END AS veces_utilizada
   FROM sensor_usuario
     JOIN sensor_entrada ON sensor_usuario.idsensor = sensor_entrada.idsensor
     JOIN entrada ON sensor_entrada.identrada = entrada.id
  GROUP BY entrada.id
  ORDER BY (
        CASE
            WHEN entrada.id = 1 THEN (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 1
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 1
              GROUP BY sensor_mcadress.idsensor)) + (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 2
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 2
              GROUP BY sensor_mcadress.idsensor))
            WHEN entrada.id = 2 THEN (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 3
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 3
              GROUP BY sensor_mcadress.idsensor)) + (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 4
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 4
              GROUP BY sensor_mcadress.idsensor))
            WHEN entrada.id = 3 THEN (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 5
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 5
              GROUP BY sensor_mcadress.idsensor)) + (( SELECT count(sensor_usuario_1.idsensor) AS count
               FROM sensor_usuario sensor_usuario_1
              WHERE sensor_usuario_1.idsensor = 6
              GROUP BY sensor_usuario_1.idsensor)) + (( SELECT count(sensor_mcadress.idsensor) AS count
               FROM sensor_mcadress
              WHERE sensor_mcadress.idsensor = 6
              GROUP BY sensor_mcadress.idsensor))
            ELSE NULL::bigint
        END) DESC;

ALTER TABLE public.veces_utilizada_entradas
    OWNER TO postgres;

