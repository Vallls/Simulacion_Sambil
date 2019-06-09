-- View: public.top5_personas_mas_visitan

-- DROP VIEW public.top5_personas_mas_visitan;

CREATE OR REPLACE VIEW public.top5_personas_mas_visitan AS
 SELECT mcadress.mcadress,
    mcadress.nombre,
    mcadress.apellido,
    count(sensor_mcadress.mcadress) / 2 AS numero_visitas
   FROM sensor_mcadress
     JOIN mcadress ON sensor_mcadress.mcadress::text = mcadress.mcadress::text
  WHERE sensor_mcadress.idsensor = 1 OR sensor_mcadress.idsensor = 2 OR sensor_mcadress.idsensor = 3 OR sensor_mcadress.idsensor = 4 OR sensor_mcadress.idsensor = 5 OR sensor_mcadress.idsensor = 6
  GROUP BY mcadress.mcadress
  ORDER BY (count(sensor_mcadress.mcadress) / 2) DESC
 LIMIT 5;

ALTER TABLE public.top5_personas_mas_visitan
    OWNER TO postgres;

