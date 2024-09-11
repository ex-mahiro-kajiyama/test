import os

from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_file,BigQueryWarehouse
from prefect_dbt.cli.commands import DbtCoreOperation
import pandas as pd

@task
def current_ls():
    dir = os.listdir("./")
    print(dir)

@task
def download_csv_from_gcs(new_file_name):
    gcsbucket = GcsBucket.load("gcsprefect")
    gcsbucket.download_object_to_path("pengin.csv",new_file_name)
    print("download csv")
    return new_file_name

@task
def csv_to_not_null_csv(file_path):
    new_file_path = f"not_null_{file_path}"
    df = pd.read_csv(file_path)
    df = df.dropna()
    df.to_csv(new_file_path,index=False)
    print("read csv return df")
    return new_file_path

@task
def csv_load_bq(filepath):
    gcpcredentails = GcpCredentials.load("gcpcredentials")
    job_config = {"write_disposition" : "WRITE_TRUNCATE"}#すでにテーブルあったら置き換える

    result = bigquery_load_file(
        dataset="prefect_test",# datasetは先に作らないと駄目です
        table="pengin",
        path=filepath,
        gcp_credentials=gcpcredentails,
        job_config=job_config,
        location='us-central1'#locationをdatasetと一致させる
    )
    print("create table")
    return result

@task
def excute_bq(sql_file_path):
    # sqlファイルを読み込む
    with open(sql_file_path,'r') as file:
        sql_query = file.read()

    with BigQueryWarehouse.load("testwarehouse") as warehouse:
        warehouse.execute(sql_query)

    print("query sql")

@task
def dbt_task():
    result = DbtCoreOperation(
        commands=["dbt run --select models/test_prefect/"],
        project_dir="./test_bigquery/",
        profiles_dir="./test_bigquery/"
    ).run()
    print("dbt run")
    return result

@flow(name="gcstest")
def gcs_test():
    current_ls()
    file_path = download_csv_from_gcs("gcs_pengin.csv")
    new_file = csv_to_not_null_csv(file_path)
    csv_load_bq(new_file)
    excute_bq("test.sql")
    dbt_task()
    print("gcs_test")

if __name__ == "__main__":
    gcs_test()
