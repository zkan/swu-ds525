# Creating and Scheduling Data Pipelines

On Linux, we need to make sure that we configure these:

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
