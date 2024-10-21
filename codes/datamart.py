from pyspark.sql import functions as F 
import pytz
from datetime import datetime, timedelta

import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from pyspark.sql.types import StringType
from awsglue.context import GlueContext
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.window import Window

glue_client = boto3.client('glue')

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

def add_partitions(database_name, table_name, partition_list):
    response = glue_client.batch_create_partition(
        DatabaseName=database_name,
        TableName=table_name,
        PartitionInputList=partition_list
    )
    return response


# TODO bucketはご自身で指定してください
bucket = "hoge.data.platform.bucket"
# TODO 要件に沿って前日分を処理するようになっています。当日分の場合は - timedelta(days=1)を削除してください
tokyo = pytz.timezone('Asia/Tokyo')
current_date_before = datetime.now(tz=tokyo) - timedelta(days=1)
# 日付を YYYY-MM-DD の形式にフォーマット
formatted_date = current_date_before.strftime('%Y-%m-%d')
spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session

# データフレームの読み込み
currency_df = spark.read.parquet(f"s3://{bucket}/gold/master.db/currency_rates/retrieval_date={formatted_date}/*.parquet")

sales_df = spark.read.parquet(f"s3://{bucket}/raw/companyA/shop/sales/{formatted_date}/*/*.parquet")

currency_df = currency_df.withColumnRenamed("retrieval_date", "retrieval_date_currency")

# データフレームの結合
joined_df = sales_df.join(currency_df, (sales_df["retrieval_date"] == currency_df["retrieval_date_currency"]) & (currency_df["currency"] == "JPY"), "inner")

# >>> joined_df.show()
# +----+--------+-----------+---------+----------+--------------------+-----+--------------+--------+------------+-----------------------+
# |page|store_id|purchase_id|    scale|store_name|                time|sales|retrieval_date|currency|       rates|retrieval_date_currency|
# +----+--------+-----------+---------+----------+--------------------+-----+--------------+--------+------------+-----------------------+
# |   2|       1|      A0022|      yen|   Store A|2024-06-12 19:45:...| 8000|    2024-06-13|     JPY|156.79364286|             2024-06-13|
# |   2|       2|      B0012|us_doller|   Store B|2024-06-12 19:20:...|20000|    2024-06-13|     JPY|156.79364286|             2024-06-13|
# |   9|       1|      A0029|      yen|   Store A|2024-06-12 19:45:...| 8000|    2024-06-13|     JPY|156.79364286|             2024-06-13|
# |   9|       2|      B0019|us_doller|   Store B|2024-06-12 19:20:...|20000|    2024-06-13|     JPY|156.79364286|             2024-06-13|
# |   5|       2|      B0025|us_doller|   Store B|2024-06-12 19:50:...|12000|    2024-06-13|     JPY|156.79364286|             2024-06-13|
# |   5|       1|      A0025|      yen|   Store A|2024-06-12 19:45:...| 8000|    2024-06-13|     JPY|156.79364286|             2024-06-13|

# 日本円に変換された売上を計算
joined_df = joined_df.withColumn("sales_jpy", 
    F.when(F.col("scale") == "yen", F.col("sales"))
    .when(F.col("scale") == "us_doller", F.col("sales") * F.col("rates"))
    .otherwise(F.col("sales"))
)

# >>> joined_df.show()
# +----+--------+-----------+---------+----------+--------------------+-----+--------------+--------+------------+-----------------------+-------------+
# |page|store_id|purchase_id|    scale|store_name|                time|sales|retrieval_date|currency|       rates|retrieval_date_currency|    sales_jpy|
# +----+--------+-----------+---------+----------+--------------------+-----+--------------+--------+------------+-----------------------+-------------+
# |   2|       1|      A0022|      yen|   Store A|2024-06-12 19:45:...| 8000|    2024-06-13|     JPY|156.79364286|             2024-06-13|       8000.0|
# |   2|       2|      B0012|us_doller|   Store B|2024-06-12 19:20:...|20000|    2024-06-13|     JPY|156.79364286|             2024-06-13| 3135872.8572|
# |   9|       1|      A0029|      yen|   Store A|2024-06-12 19:45:...| 8000|    2024-06-13|     JPY|156.79364286|             2024-06-13|       8000.0|
# |   9|       2|      B0019|us_doller|   Store B|2024-06-12 19:20:...|20000|    2024-06-13|     JPY|156.79364286|             2024-06-13| 3135872.8572|
# |   5|       2|      B0025|us_doller|   Store B|2024-06-12 19:50:...|12000|    2024-06-13|     JPY|156.79364286|             2024-06-13|1881523.71432|
# |   5|       1|      A0025|      yen|   Store A|2024-06-12 19:45:...| 8000|    2024-06-13|     JPY|156.79364286|             2024-06-13|       8000.0|

# 日付と時間の分離
joined_df = joined_df.withColumn("time_jst", F.from_utc_timestamp(F.col("time"), "Asia/Tokyo"))
joined_df = joined_df.withColumn("ymd", F.to_date(F.col("time_jst")))
joined_df = joined_df.withColumn("hh", F.hour(F.col("time_jst")).cast(StringType()))
joined_df = joined_df.withColumn("store_id", F.col("store_id").cast("string"))

# >>> joined_df.show(truncate=False)
# +----+--------+-----------+---------+----------+-----------------------+-----+--------------+--------+------------+-----------------------+-------------+-----------------------+----------+---+
# |page|store_id|purchase_id|scale    |store_name|time                   |sales|retrieval_date|currency|rates       |retrieval_date_currency|sales_jpy    |time_jst               |ymd       |hh |
# +----+--------+-----------+---------+----------+-----------------------+-----+--------------+--------+------------+-----------------------+-------------+-----------------------+----------+---+
# |2   |1       |A0022      |yen      |Store A   |2024-06-12 19:45:30.456|8000 |2024-06-13    |JPY     |156.79364286|2024-06-13             |8000.0       |2024-06-13 04:45:30.456|2024-06-13|4  |
# |2   |2       |B0012      |us_doller|Store B   |2024-06-12 19:20:35.456|20000|2024-06-13    |JPY     |156.79364286|2024-06-13             |3135872.8572 |2024-06-13 04:20:35.456|2024-06-13|4  |
# |9   |1       |A0029      |yen      |Store A   |2024-06-12 19:45:30.456|8000 |2024-06-13    |JPY     |156.79364286|2024-06-13             |8000.0       |2024-06-13 04:45:30.456|2024-06-13|4  |
# |9   |2       |B0019      |us_doller|Store B   |2024-06-12 19:20:35.456|20000|2024-06-13    |JPY     |156.79364286|2024-06-13             |3135872.8572 |2024-06-13 04:20:35.456|2024-06-13|4  |
# |5   |2       |B0025      |us_doller|Store B   |2024-06-12 19:50:30.789|12000|2024-06-13    |JPY     |156.79364286|2024-06-13             |1881523.71432|2024-06-13 04:50:30.789|2024-06-13|4  |
# |5   |1       |A0025      |yen      |Store A   |2024-06-12 19:45:30.456|8000 |2024-06-13    |JPY     |156.79364286|2024-06-13             |8000.0       |2024-06-13 04:45:30.456|2024-06-13|4  |

# 必要な列だけを選択
result_df = joined_df.select("store_id", "retrieval_date", "page", "purchase_id", "scale", "store_name", "time", "sales", "sales_jpy", "ymd", "hh")

#### 品質検証
# purchase_idがユニークであるかチェック
window_spec = Window.partitionBy("purchase_id")
# purchase_idごとの件数を数える
result_df = result_df.withColumn("count", F.count("purchase_id").over(window_spec))

# グループごとのpurchase_idが1件以上あるデータを抜き出す
non_unique_ids = result_df.filter(F.col("count") > 1).select("purchase_id").distinct().collect()

# データが一件でもあればエラーとする
if non_unique_ids:
    non_unique_ids = [row.purchase_id for row in non_unique_ids]
    # 今回はエラーとしたがワーニングとして処理を継続しても良い
    raise ValueError(f"Non-unique purchase_id found: {non_unique_ids}")

######

# 書き込み
result_df.write.partitionBy(["store_id"]).mode("overwrite").option("compression", "snappy").parquet(f"s3://{bucket}/gold/companyA/shop/sales/asset/money.db/salesdata/retrieval_date={formatted_date}/")

partitions = result_df.select("store_id", "retrieval_date").distinct().collect()
partition_list = []

for partition in partitions:
    retrieval_date = partition['retrieval_date']
    store_id = partition['store_id']
    if not partition_exists("money", "salesdata", [retrieval_date, store_id]):
        partition_list.append({
            'Values': [retrieval_date, store_id],
            'StorageDescriptor': {
                'Location': f's3://{bucket}/gold/companyA/shop/sales/asset/money.db/salesdata/retrieval_date={retrieval_date}/store_id={store_id}',
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat':'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'Compressed': False,
                'SerdeInfo':{'Name':'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe','SerializationLibrary':'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'}
            }
        })

print(partition_list)

if partition_list:
    add_partitions("money", "salesdata", partition_list)
