import csv
import glob
import json
import os
from typing import List

from google.cloud import bigquery
from google.oauth2 import service_account


def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def main(dataset_id, table_id, file_path):
    # โค้ดส่วนนี้จะเป็นการใช้ Keyfile เพื่อสร้าง Credentials เอาไว้เชื่อมต่อกับ BigQuery
    # โดยการสร้าง Keyfile สามารถดูได้จากลิ้งค์ About Google Cloud Platform (GCP)
    # ที่หัวข้อ How to Create Service Account
    #
    # การจะใช้ Keyfile ได้ เราต้องกำหนด File Path ก่อน ซึ่งวิธีกำหนด File Path เราสามารถ
    # ทำได้โดยการเซตค่า Environement Variable ที่ชื่อ KEYFILE_PATH ได้ จะทำให้เวลาที่เราปรับ
    # เปลี่ยน File Path เราจะได้ไม่ต้องกลับมาแก้โค้ด
    # keyfile = os.environ.get("KEYFILE_PATH")
    #
    # แต่เพื่อความง่ายเราสามารถกำหนด File Path ไปได้เลยตรง ๆ
    keyfile = "YOUR_KEYFILE_PATH"
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    # โค้ดส่วนนี้จะเป็นการสร้าง Client เชื่อมต่อไปยังโปรเจค GCP ของเรา โดยใช้ Credentials ที่
    # สร้างจากโค้ดข้างต้น
    project_id = "YOUR_GCP_PROJECT"
    client = bigquery.Client(
        project=project_id,
        credentials=credentials,
    )

    # โค้ดส่วนนี้เป็นการ Configure Job ที่เราจะส่งไปทำงานที่ BigQuery โดยหลัก ๆ เราก็จะกำหนดว่า
    # ไฟล์ที่เราจะโหลดขึ้นไปมีฟอร์แมตอะไร มี Schema หน้าตาประมาณไหน
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        schema=[
            bigquery.SchemaField("id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("type", bigquery.SqlTypeNames.STRING),
        ],
    )

    # โค้ดส่วนนี้จะเป็นการอ่านไฟล์ CSV และโหลดขึ้นไปยัง BigQuery
    with open(file_path, "rb") as f:
        table_id = f"{project_id}.{dataset_id}.{table_id}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

    # โค้ดส่วนนี้จะเป็นการดึงข้อมูลจากตารางที่เราเพิ่งโหลดข้อมูลเข้าไป เพื่อจะตรวจสอบว่าเราโหลดข้อมูล
    # เข้าไปทั้งหมดกี่แถว มีจำนวน Column เท่าไร
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


if __name__ == "__main__":
    all_files = get_files(filepath="../data")
    print(all_files)

    with open("github_events.csv", "w") as csv_file:
        writer = csv.writer(csv_file)

        for datafile in all_files:
            with open(datafile, "r") as f:
                data = json.loads(f.read())
                for each in data:
                    writer.writerow([each["id"], each["type"]])

    main(dataset_id, table_id, file_path)
