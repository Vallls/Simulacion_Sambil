-- View: public.top5_tiendas_mas_compradas

-- DROP VIEW public.top5_tiendas_mas_compradas;

CREATE OR REPLACE VIEW public.top5_tiendas_mas_compradas AS
 SELECT tienda.nombre,
    count(compra.idtienda) AS numero_veces_comprado
   FROM compra
     JOIN tienda ON compra.idtienda = tienda.id
  GROUP BY tienda.id
  ORDER BY (count(compra.idtienda)) DESC
 LIMIT 5;

ALTER TABLE public.top5_tiendas_mas_compradas
    OWNER TO postgres;

