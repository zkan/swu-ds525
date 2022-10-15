from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator


def _push(**context):
    # ti = context["ti"]
    # ti.xcom_push(key="name", value="Kan")
    return ["Kan", "Chin", "Peeyapak", "Lyn", "Peerawit", "Kruewan", "Pongthanin", "Mindii", "Nuttapol", "Nattakan",]


def _pull(**context):
    ti = context["ti"]
    ds = context["ds"]

    names = ti.xcom_pull(task_ids="push", key="return_value")

    for each in names:
        print(f"Hello {each} on {ds}")


with DAG(
    "test_xcom",
    start_date=timezone.datetime(2022, 10, 15),
    schedule="@daily",
    tags=["workshop"],
    catchup=False,
) as dag:

    push = PythonOperator(
        task_id="push",
        python_callable=_push,
    )

    pull = PythonOperator(
        task_id="pull",
        python_callable=_pull,
    )

    push >> pull