import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, TimestampType
import pytz
import json
from datetime import datetime, timedelta
import random

# セッション生成
spark = SparkSession.builder.appName("SalesData").getOrCreate()
access_token_cast=""

# トークンの取得
def get_access_token():

    # TODO 実際は以下のようにリクエストを送信し呼び出し
    # リクエストデータ
    # data = {
    #     'grant_type': 'password',
    #     'username': 'hoge',
    #     'password': 'peke',
    #     'client_id': 'clientid',
    #     'client_secret': 'secret'
    # }
    # # トークンを取得（架空のAPIなので動かない）
    # url = 'https://hoge.token.token.jp/oauth2/token'

    # # HTTP POSTリクエストを送信し、レスポンスを取得
    # response = requests.post(url, data=data)
    # # レスポンスのステータスコードを確認
    # if response.status_code == 200:
    #     # レスポンスからaccess_tokenを抜き出す
    #     json_response = response.json()
    #     access_token = json_response.get('access_token')
    #     if access_token:
    #         print("Access Token:", access_token)
    #     else:
    #         print("Access Token not found in the response.")
    # else:
    #     # レスポンスが失敗した場合はエラーメッセージを表示
    #     print("Error:", response.status_code)

    # 架空のJWTトークンが得られたものとする
    access_token = 'eyJhbGciOiJIUzI1NiIsICJ0eXAiOiJKV1QifQ==.eyJzdWIiOiIwMDEyMzQiLCJuYW1lIjoiVGFybyBMaWciLCJpYXQiOiIxNjcxNTk2MDY2In0=.bdlILb+uDIaIDRsZbxzQYjz+yB8UQGUu44fHyVhMbq4='
    
    return access_token

# UDFを定義して各ページのデータを取得
def fetch_page_data(page):

    # TODO 今回はAPIが架空だが、本来なら以下のような取得イメージ
    access_token = access_token_cast.value
    print("アクセストークン:" + access_token)
    # headers = {
    #     'Content-Type': 'application/json',
    #     'Authorization': f'Bearer {access_token}'
    # }
    # data = {
    #     "startTime": "2024-06-01 12:00:00",
    #     "endTime": "2024-06-01 13:00:00",
    #     "page": page
    # }
    #response = requests.post(url, headers=headers, json=data)
    #if response.status_code == 200:
    #  return json.dumps(response.json().get("data", []))
    #else:
    #  return json.dumps([])

    # TODO 半固定のJSONレスポンスがあるとする
    # ページ番号がpurchase_idに含まれるようにして、データをページごとに変えている

    #tokyo = pytz.timezone('Asia/Tokyo')
    # データはUTCで取得されるためUTCで
    current_date = datetime.now() - timedelta(hours=1)
    # 日付を YYYY-MM-DD の形式にフォーマット
    formatted_date = current_date.strftime('%Y-%m-%d')
    formatted_date_hour = current_date.strftime('%Y-%m-%d-%H')
    formatted_hour = current_date.strftime('%H')

    random_number1 = random.randint(1000, 9999)
    random_number2 = random.randint(1000, 9999)
    random_number3 = random.randint(10, 99)
    random_number4 = random.randint(10, 99)

    per_page = 1000
    total_pages = 10
    total_items = 10000
    data = [
        {
            "store_id": 1,
            "store_name": "Store A",
            "purchase_id": f"A001{page}_{formatted_date_hour}",
            "sales": random_number1,
            "time": f"{formatted_date}T{formatted_hour}:15:30.123Z",
            "scale": "yen"
        },
        {
            "store_id": 1,
            "store_name": "Store A",
            "purchase_id": f"A002{page}_{formatted_date_hour}",
            "sales": random_number2,
            "time": f"{formatted_date}T{formatted_hour}:45:30.456Z",
            "scale": "yen"
        },
        {
            "store_id": 2,
            "store_name": "Store B",
            "purchase_id": f"B001{page}_{formatted_date_hour}",
            "sales": random_number3,
            "time": f"{formatted_date}T{formatted_hour}:20:35.456Z",
            "scale": "us_doller"
        },
        {
            "store_id": 2,
            "store_name": "Store B",
            "purchase_id": f"B002{page}_{formatted_date_hour}",
            "sales": random_number4,
            "time": f"{formatted_date}T{formatted_hour}:50:30.789Z",
            "scale": "us_doller"
        }
    ]

    # 変数をJSONデータに埋め込む
    response = f"""
    {{
    "page": {page},
    "per_page": {per_page},
    "total_pages": {total_pages},
    "total_items": {total_items},
    "data": {json.dumps(data, indent=4)}
    }}
    """
    
    return response

if __name__ == '__main__':

    # TODO 
    bucket = "hoge.data.platform.bucket"

    # トークンを取得する
    token = get_access_token()
    # トークンをブロードキャスト変数に設定
    access_token_cast = spark.sparkContext.broadcast(token)

    # # リクエストを送信するURL
    # url = 'http://shop.hoge.com/openapi/v1/shops/salesdata'

    # # 初期リクエストデータ
    # initial_data = {
    #     "startTime": "2024-06-01 12:00:00",
    #     "endTime": "2024-06-01 13:00:00",
    #     "page": 1
    # }

    # # 最初のリクエストを送信してtotal_pagesを取得
    # headers = {
    #     'Content-Type': 'application/json',
    #     'Authorization': f'Bearer {access_token}'
    # }

    #response = requests.post(url, headers=headers, json=initial_data)
    #response_data = response.json()
    #total_pages = response_data["total_pages"]

    #TODO 今回は架空APIなので10ページとする
    total_pages = 10

    # SparkのUDFを登録する
    fetch_page_data_udf = F.udf(fetch_page_data, StringType())

    # ページ数分だけのDataFrameを生成
    pages_df = spark.range(1, total_pages + 1).toDF("page")
    # >>> pages_df.show()
    # +----+
    # |page|
    # +----+
    # |   1|
    # |   2|
    # |   3|
    # |   4|
    # |   5|
    # |   6|
    # |   7|
    # |   8|
    # |   9|
    # |  10|
    # +----+

    # 各ページのデータをページごとに並列分散で取得してDataFrameに変換
    data_df = pages_df.withColumn("sales_data", fetch_page_data_udf(pages_df.page))

    # >>> data_df.show(truncate=False)
    # +----+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    # |page|sales_data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
    # +----+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    # |1   |{per_page=1000, total_pages=2, page=1, data=[{store_id=1, purchase_id=A0011, scale=yen, store_name=Store A, time=2024-05-01T10:15:30.123Z, sales=15000}, {store_id=1, purchase_id=A0021, scale=yen, store_name=Store A, time=2024-05-01T12:45:30.456Z, sales=8000}, {store_id=2, purchase_id=B0011, scale=us_doller, store_name=Store B, time=2024-05-01T11:20:35.456Z, sales=20000}, {store_id=2, purchase_id=B0021, scale=us_doller, store_name=Store B, time=2024-05-01T14:50:30.789Z, sales=12000}], total_items=2000}|
    # |2   |{per_page=1000, total_pages=2, page=2, data=[{store_id=1, purchase_id=A0012, scale=yen, store_name=Store A, time=2024-05-01T10:15:30.123Z, sales=15000}, {store_id=1, purchase_id=A0022, scale=yen, store_name=Store A, time=2024-05-01T12:45:30.456Z, sales=8000}, {store_id=2, purchase_id=B0012, scale=us_doller, store_name=Store B, time=2024-05-01T11:20:35.456Z, sales=20000}, {store_id=2, purchase_id=B0022, scale=us_doller, store_name=Store B, time=2024-05-01T14:50:30.789Z, sales=12000}], total_items=2000}|
    # +----+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    
    # JSON文字列を展開してDataFrameに変換するスキーマを定義
    schema = StructType([
        StructField("per_page", IntegerType(), True),
        StructField("total_pages", IntegerType(), True),
        StructField("page", IntegerType(), True),
        StructField("data", ArrayType(StructType([
            StructField("store_id", IntegerType(), True),
            StructField("purchase_id", StringType(), True),
            StructField("scale", StringType(), True),
            StructField("store_name", StringType(), True),
            StructField("time", TimestampType(), True),
            StructField("sales", DoubleType(), True)
        ])), True),
        StructField("total_items", IntegerType(), True)
    ])

    df = data_df.withColumn("sales_data", F.from_json(data_df["sales_data"], schema))
    # dataフィールドを展開
    df = df.withColumn("data", F.explode(F.col("sales_data.data")))
    # 必要なフィールドを選択し、データを操作しやすくする
    result_df = df.select("page", "data.*")
    # >>> result_df.show(truncate=False)
    # +----+--------+-----------+---------+----------+-----------------------+-----+
    # |page|store_id|purchase_id|scale    |store_name|time                   |sales|
    # +----+--------+-----------+---------+----------+-----------------------+-----+
    # |1   |1       |A0011      |yen      |Store A   |2024-06-12 19:15:30.123|15000|
    # |1   |1       |A0021      |yen      |Store A   |2024-06-12 19:45:30.456|8000 |
    # |1   |2       |B0011      |us_doller|Store B   |2024-06-12 19:20:35.456|20000|
    # |1   |2       |B0021      |us_doller|Store B   |2024-06-12 19:50:30.789|12000|
    # |2   |1       |A0012      |yen      |Store A   |2024-06-12 19:15:30.123|15000|
    # |2   |1       |A0022      |yen      |Store A   |2024-06-12 19:45:30.456|8000 |
    # |2   |2       |B0012      |us_doller|Store B   |2024-06-12 19:20:35.456|20000|
    # |2   |2       |B0022      |us_doller|Store B   |2024-06-12 19:50:30.789|12000|
    # +----+--------+-----------+---------+----------+-----------------------+-----+

    # retrieval_dateを付与(再実行等を考えるとcurrent_timestampでない方が好ましいがここでは、Job起動日時とする)
    result_df = result_df.withColumn("retrieval_date", F.date_format(F.from_utc_timestamp(F.current_timestamp(), "Asia/Tokyo"), "yyyy-MM-dd"))
    # +----+--------+-----------+---------+----------+-----------------------+-----+--------------+
    # |page|store_id|purchase_id|scale    |store_name|time                   |sales|retrieval_date|
    # +----+--------+-----------+---------+----------+-----------------------+-----+--------------+
    # |1   |1       |A0011      |yen      |Store A   |2024-06-12 19:15:30.123|15000|2024-06-12 |
    # |1   |1       |A0021      |yen      |Store A   |2024-06-12 19:45:30.456|8000 |2024-06-12 |
    # |1   |2       |B0011      |us_doller|Store B   |2024-06-12 19:20:35.456|20000|2024-06-12 |
    # |1   |2       |B0021      |us_doller|Store B   |2024-06-12 19:50:30.789|12000|2024-06-12 |
    # |2   |1       |A0012      |yen      |Store A   |2024-06-12 19:15:30.123|15000|2024-06-12 |
    # |2   |1       |A0022      |yen      |Store A   |2024-06-12 19:45:30.456|8000 |2024-06-12 |
    # |2   |2       |B0012      |us_doller|Store B   |2024-06-12 19:20:35.456|20000|2024-06-12 |
    # |2   |2       |B0022      |us_doller|Store B   |2024-06-12 19:50:30.789|12000|2024-06-12 |
    # +----+--------+-----------+---------+----------+-----------------------+-----+--------------+
    # 現在の日付を取得
    tokyo = pytz.timezone('Asia/Tokyo')
    current_date = datetime.now(tz=tokyo) - timedelta(hours=1)
    # 日付を YYYY-MM-DD の形式にフォーマット
    formatted_date = current_date.strftime('%Y-%m-%d')
    formatted_hour = current_date.strftime('%H')
    # データをrawゾーンへ保存する
    # スモールファイルにならないように2つにまとめる
    result_df.repartition(2).write.mode("overwrite").option("compression", "snappy").parquet(f"s3://{bucket}/raw/companyA/shop/sales/{formatted_date}/{formatted_hour}/")