```bash
pip instll dbt-core
pip install dbt-bigquery
```

Initialize a dbt project

```bash
dbt init DEZ_hw4
```

Change the project dataset region to match the GCS external table region 
```bash 
vi ~/.dbt/profile.yaml
```

There are 24 external tables in bigquery for green and yellow tripdata respectively. For dbt can easy to process all the tripdata, create a main table view for yellow and green tripdata
```bash
gcloud beta interactive
```

```bash
bq query --use_legacy_sql=false --location=australia-southeast1 'CREATE OR REPLACE VIEW `dez-jimmyh.w2_kestra_dataset.green_tripdata_view` AS
SELECT * FROM `dez-jimmyh.w2_kestra_dataset.green_2019-01_tripdata_ext`
UNION ALL
SELECT * FROM `dez-jimmyh.w2_kestra_dataset.green_2019-02_tripdata_ext`
UNION ALL
...
UNION ALL
SELECT * FROM `dez-jimmyh.w2_kestra_dataset.green_2020-11_tripdata_ext`
UNION ALL
SELECT * FROM `dez-jimmyh.w2_kestra_dataset.green_2020-12_tripdata_ext`;'
```

Q1:
**Ans:select * from myproject.my_nyc_tripdata.ext_green_taxi**
When DBT is executed, it tries to read env variable in order:

1. Reading environment variables from Shell/Terminal
2. Read from .env or dbt_project.yml
3. Use a preset value provided by env_var() (e.g. 'dtc_zoomcamp_2025')

Therefore, onece seting the env variables at terminal, the evn_var would use the variables of terminal instead the default value, 

- env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') →  "myproject"
- env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') →  "my_nyc_tripdata"

```sql
select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```

would compile as

```sql
select * 
from `myproject.my_nyc_tripdata.ext_green_taxi`
```

Q2:
**Ans:**