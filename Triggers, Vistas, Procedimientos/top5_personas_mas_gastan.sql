-- View: public.top5_personas_mas_gastan

-- DROP VIEW public.top5_personas_mas_gastan;

CREATE OR REPLACE VIEW public.top5_personas_mas_gastan AS
 SELECT mcadress.mcadress,
    mcadress.nombre,
    mcadress.apellido,
    sum(compra.monto) AS cantidad
   FROM compra
     JOIN mcadress ON compra.mcadress::text = mcadress.mcadress::text
  WHERE compra.mcadress IS NOT NULL
  GROUP BY mcadress.mcadress
  ORDER BY (sum(compra.monto)) DESC
 LIMIT 5;

ALTER TABLE public.top5_personas_mas_gastan
    OWNER TO postgres;

