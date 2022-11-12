from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator


def _say_hello():
    return "Hello!"


with DAG(
    "hello",
    start_date=timezone.datetime(2022, 11, 1),
    schedule="@daily",
    tags=["workshop"],
):

    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=_say_hello,
    )

    say_hello_2 = PythonOperator(
        task_id="say_hello_2",
        python_callable=_say_hello,
    )

    say_hello_3 = PythonOperator(
        task_id="say_hello_3",
        python_callable=_say_hello,
    )

    say_hello >> say_hello_2 >> say_hello_3