import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import pytz
import boto3
from botocore.exceptions import ClientError

# パーティションが存在するかどうかを確認する関数
def partition_exists(database_name, table_name, partition_values):
    try:
        glue_client.get_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionValues=partition_values
        )
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False
        else:
            raise

glue_client = boto3.client('glue')

# TODO buketはご自身で指定してください
bucket = "hoge.data.platform.bucket"
# TODO https://openexchangerates.org/signupよりFree Planより契約しApp IDsを取得し以下に設定してください
your_app_id = 'd527c183c99a4a8aa28198b1ac1a7770'
response = requests.get(f"https://openexchangerates.org/api/latest.json?app_id={your_app_id}")

# タイムスタンプを日付に変換
timestamp = response.json()["timestamp"]
tokyo = pytz.timezone('Asia/Tokyo')
# 翌日にワークフローが起動するので、取得日を1日前に設定
jst_dt = datetime.fromtimestamp(timestamp, tz=tokyo) - timedelta(days=1)
retrieval_date = jst_dt.strftime('%Y-%m-%d')

# レートデータをデータフレームに変換
rates =  response.json()["rates"]
df = pd.DataFrame(rates.items(), columns=["currency", "rates"])

# 取得日を追加
df["retrieval_date"] = retrieval_date
# Parquet形式で出力※
output_file = f"s3://{bucket}/gold/master.db/currency_rates/retrieval_date={retrieval_date}/exchange_rates.parquet"
df.to_parquet(output_file, compression='snappy', index=False)

# ※ Apache Pyarrowライブラリによるparquet出力
# pandasなどの分析向けライブラリを利用してparquetファイルを読み込んだり、分析の結果をparquetで出力することも可能です。

# partitionの追加
# データとメタデータ(テーブル定義)の管理場所が明確に分離されているため、S3に配置しただけでは、テーブルがデータを配置されたことを検知できない。(p67　ロケーション参照)
# そのため明示的にテーブルに対してpartition情報を追加する

if not partition_exists("master", "currency_rates", [retrieval_date]):
    response = glue_client.create_partition(
        DatabaseName="master",
        TableName="currency_rates",
        PartitionInput={
            'Values': [retrieval_date],
            'StorageDescriptor': {
                'SerdeInfo':{'Name':'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe','SerializationLibrary':'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'},
                'Location': f"s3://{bucket}/gold/master.db/currency_rates/retrieval_date={retrieval_date}/",
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat':'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            }
        }
    )


