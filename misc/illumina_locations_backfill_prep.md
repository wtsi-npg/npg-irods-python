A list of 'qc complete' runs not already present in the
`iseq_product_locations_irods` table was generated using the following
query:

```
SELECT DISTINCT
  irlm.id_run, 
  irlm.instrument_model
FROM 
  iseq_run_lane_metrics AS irlm 
  JOIN iseq_run_status AS irs
    ON irlm.id_run=irs.id_run
  JOIN iseq_run_status_dict AS irsd
    ON irsd.id_run_status_dict=irs.id_run_status_dict
WHERE 
  irsd='qc complete'
  AND irlm.id_run NOT IN
    (
      SELECT DISTINCT
        ipm.id_run
      FROM 
        iseq_product_metrics AS ipm 
        JOIN seq_product_irods_locations AS spil
          ON ipm.id_iseq_product=spil.id_product
    );
```

This list was then split by instrument type, to provide a list of
NovaSeq runs and a list of runs from all other instrument types, to
determine the path where the data should be found.



