  {{ config(
    unique_key='liability_id'
) }}
  
  SELECT * 
  FROM (
    VALUES 
    (1, 'low'),
    (2, 'mid'),
    (3, 'high')
  ) AS t(liability_id,liability_name)
