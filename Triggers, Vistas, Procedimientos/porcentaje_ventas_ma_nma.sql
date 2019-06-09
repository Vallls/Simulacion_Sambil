-- View: public.porcentaje_ventas_ma_nma

-- DROP VIEW public.porcentaje_ventas_ma_nma;

CREATE OR REPLACE VIEW public.porcentaje_ventas_ma_nma AS
 SELECT (( SELECT count(compra_1.factura) AS count
           FROM compra compra_1
          WHERE compra_1.mcadress IS NULL)) * 100 / (( SELECT count(compra_1.factura) AS count
           FROM compra compra_1)) AS is_null_porcentaje,
    (( SELECT count(compra_1.factura) AS count
           FROM compra compra_1
          WHERE compra_1.mcadress IS NOT NULL)) * 100 / (( SELECT count(compra_1.factura) AS count
           FROM compra compra_1)) AS is_not_null_porcentaje
   FROM compra
  GROUP BY ((( SELECT count(compra_1.factura) AS count
           FROM compra compra_1
          WHERE compra_1.mcadress IS NULL)) * 100 / (( SELECT count(compra_1.factura) AS count
           FROM compra compra_1)));

ALTER TABLE public.porcentaje_ventas_ma_nma
    OWNER TO postgres;

