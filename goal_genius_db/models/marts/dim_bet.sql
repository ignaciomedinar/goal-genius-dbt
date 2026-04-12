{{ config(
        materialized='table',
        unique_key='bet_id'
) }}

SELECT * 
  FROM (
    VALUES 
    (1, 'home'),
    (2, 'away'),
    (3, 'draw'),
    (4, 'draw->home'),
    (5, 'draw->away')
  ) AS t(bet_id,bet_name)