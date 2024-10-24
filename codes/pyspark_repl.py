#　Sparkインストール(https://spark.apache.org/downloads.html)「Installing with PyPi」を利用すると楽です
# spark repl起動のコマンド
#pyspark --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import uuid

# サンプルデータの作成
data = [
    (str(uuid.uuid4()), "Alice", 34, "2024-01-01"),
    (str(uuid.uuid4()), "Bob", 45, "2024-01-02"),
    (str(uuid.uuid4()), "Cathy", 29, "2024-01-03")
]

columns = ["uuid", "name", "age", "ts"]

df = spark.createDataFrame(data, columns)

# +------------------------------------+-----+---+----------+
# |uuid                                |name |age|ts        |
# +------------------------------------+-----+---+----------+
# |719c6e15-de28-4f26-acb4-5cbac310f455|Alice|34 |2024-01-01|
# |15875a44-6e4c-4f2a-8097-97989722a38d|Bob  |45 |2024-01-02|
# |da02da2b-43fa-41f0-b65e-de20258393b3|Cathy|29 |2024-01-03|
# +------------------------------------+-----+---+----------+

# # Hudiテーブルのパス
## TODO ご自身のパスに変えてください
hudi_table_path = "/Users/yuki/Desktop/dataplatform/"

# Hudiテーブルの作成（初回書き込み）
df.write.format("hudi") \
    .option("hoodie.table.name", "hudi_test_table") \
    .option("hoodie.datasource.write.recordkey.field", "uuid") \
    .option("hoodie.datasource.write.partitionpath.field", "ts") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .mode("overwrite") \
    .save(hudi_table_path)

# 追加するデータの作成（更新データ）
update_data = [
    (str(uuid.uuid4()), "David", 42, "2024-01-04"),  # 新規データ
    (df.first()["uuid"], "Alice", 35, "2024-01-01")  # Aliceのデータを更新
]

update_df = spark.createDataFrame(update_data, columns)

# >>> update_df.show(truncate=False)
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+
# |_hoodie_commit_time|_hoodie_commit_seqno |_hoodie_record_key                  |_hoodie_partition_path|_hoodie_file_name                                                        |uuid                                |name |age|ts        |
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+
# |20240618112155544  |20240618112155544_2_0|242a244d-bac4-4784-8fc3-6c880411accf|2024-01-03            |30ce8c3c-1909-4849-8084-b5401eb2d637-0_2-62-479_20240618112155544.parquet|242a244d-bac4-4784-8fc3-6c880411accf|Cathy|29 |2024-01-03|
# |20240618112155544  |20240618112155544_1_0|97ad882b-1de7-4393-acc1-f9c09fed7336|2024-01-01            |de1c4eee-4164-4b32-b430-65c891568bc1-0_1-62-478_20240618112155544.parquet|97ad882b-1de7-4393-acc1-f9c09fed7336|Alice|34 |2024-01-01|
# |20240618112155544  |20240618112155544_0_0|316cef8b-d889-4246-b65e-caf8fd10f930|2024-01-02            |3e8b5791-094e-4eb9-a738-440c2a2b712f-0_0-62-477_20240618112155544.parquet|316cef8b-d889-4246-b65e-caf8fd10f930|Bob  |45 |2024-01-02|
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+

# Hudiテーブルの更新（アップサート）
update_df.write.format("hudi") \
    .option("hoodie.table.name", "hudi_test_table") \
    .option("hoodie.datasource.write.recordkey.field", "uuid") \
    .option("hoodie.datasource.write.partitionpath.field", "ts") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .mode("append") \
    .save(hudi_table_path)

# >>> update_df.show(truncate=False)
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+
# |_hoodie_commit_time|_hoodie_commit_seqno |_hoodie_record_key                  |_hoodie_partition_path|_hoodie_file_name                                                        |uuid                                |name |age|ts        |
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+
# |20240618112300299  |20240618112300299_1_0|e50b97db-afdd-41a3-b338-dc5f34417acb|2024-01-04            |66e48fb4-3ea7-48ae-a675-83f636d17fa8-0_1-92-573_20240618112300299.parquet|e50b97db-afdd-41a3-b338-dc5f34417acb|David|42 |2024-01-04|
# |20240618112155544  |20240618112155544_2_0|242a244d-bac4-4784-8fc3-6c880411accf|2024-01-03            |30ce8c3c-1909-4849-8084-b5401eb2d637-0_2-62-479_20240618112155544.parquet|242a244d-bac4-4784-8fc3-6c880411accf|Cathy|29 |2024-01-03|
# |20240618112300299  |20240618112300299_0_0|97ad882b-1de7-4393-acc1-f9c09fed7336|2024-01-01            |de1c4eee-4164-4b32-b430-65c891568bc1-0_0-92-572_20240618112300299.parquet|97ad882b-1de7-4393-acc1-f9c09fed7336|Alice|35 |2024-01-01|
# |20240618112155544  |20240618112155544_0_0|316cef8b-d889-4246-b65e-caf8fd10f930|2024-01-02            |3e8b5791-094e-4eb9-a738-440c2a2b712f-0_0-62-477_20240618112155544.parquet|316cef8b-d889-4246-b65e-caf8fd10f930|Bob  |45 |2024-01-02|
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+

# incremental Query
incremental_df = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", "20240618112300298") \
    .option("hoodie.datasource.read.end.instanttime", "20240618112300299") \
    .load(hudi_table_path)

# >>> incremental_df.show(truncate=False)
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+----+
# |_hoodie_commit_time|_hoodie_commit_seqno |_hoodie_record_key                  |_hoodie_partition_path|_hoodie_file_name                                                        |uuid                                |name |age|ts        |add |
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+----+
# |20240618112300299  |20240618112300299_1_0|e50b97db-afdd-41a3-b338-dc5f34417acb|2024-01-04            |66e48fb4-3ea7-48ae-a675-83f636d17fa8-0_1-92-573_20240618112300299.parquet|e50b97db-afdd-41a3-b338-dc5f34417acb|David|42 |2024-01-04|null|
# |20240618112300299  |20240618112300299_0_0|97ad882b-1de7-4393-acc1-f9c09fed7336|2024-01-01            |de1c4eee-4164-4b32-b430-65c891568bc1-0_0-92-572_20240618112300299.parquet|97ad882b-1de7-4393-acc1-f9c09fed7336|Alice|35 |2024-01-01|null|
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+----+

incremental_df = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", "20240618112155543") \
    .option("hoodie.datasource.read.end.instanttime", "20240618112155544") \
    .load(hudi_table_path)

# >>> incremental_df.show(truncate=False)
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+----+
# |_hoodie_commit_time|_hoodie_commit_seqno |_hoodie_record_key                  |_hoodie_partition_path|_hoodie_file_name                                                        |uuid                                |name |age|ts        |add |
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+----+
# |20240618112155544  |20240618112155544_2_0|242a244d-bac4-4784-8fc3-6c880411accf|2024-01-03            |30ce8c3c-1909-4849-8084-b5401eb2d637-0_2-62-479_20240618112155544.parquet|242a244d-bac4-4784-8fc3-6c880411accf|Cathy|29 |2024-01-03|null|
# |20240618112155544  |20240618112155544_1_0|97ad882b-1de7-4393-acc1-f9c09fed7336|2024-01-01            |de1c4eee-4164-4b32-b430-65c891568bc1-0_1-62-478_20240618112155544.parquet|97ad882b-1de7-4393-acc1-f9c09fed7336|Alice|34 |2024-01-01|null|
# |20240618112155544  |20240618112155544_0_0|316cef8b-d889-4246-b65e-caf8fd10f930|2024-01-02            |3e8b5791-094e-4eb9-a738-440c2a2b712f-0_0-62-477_20240618112155544.parquet|316cef8b-d889-4246-b65e-caf8fd10f930|Bob  |45 |2024-01-02|null|
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+----+

# スキーマエボリューション
evolution = [
    (df.first()["uuid"], "Alice", 35, "2024-01-01", "hoge")  # Aliceのデータを更新
]


columns2 = ["uuid", "name", "age", "ts", "add"]
evolution_df = spark.createDataFrame(evolution, columns2)

evolution_df.write.format("hudi") \
    .option("hoodie.table.name", "hudi_test_table") \
    .option("hoodie.datasource.write.recordkey.field", "uuid") \
    .option("hoodie.datasource.write.partitionpath.field", "ts") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .mode("append") \
    .save(hudi_table_path)

result = spark.read.format("hudi").option("hoodie.datasource.query.type", "snapshot").load(hudi_table_path)

# >>> result.show(truncate=False)
# +-------------------+---------------------+------------------------------------+----------------------+--------------------------------------------------------------------------+------------------------------------+-----+---+----+----------+
# |_hoodie_commit_time|_hoodie_commit_seqno |_hoodie_record_key                  |_hoodie_partition_path|_hoodie_file_name                                                         |uuid                                |name |age|add |ts        |
# +-------------------+---------------------+------------------------------------+----------------------+--------------------------------------------------------------------------+------------------------------------+-----+---+----+----------+
# |20240618114348496  |20240618114348496_0_0|97ad882b-1de7-4393-acc1-f9c09fed7336|2024-01-01            |de1c4eee-4164-4b32-b430-65c891568bc1-0_0-173-800_20240618114348496.parquet|97ad882b-1de7-4393-acc1-f9c09fed7336|Alice|35 |hoge|2024-01-01|
# |20240618113730201  |20240618113730201_1_0|e50b97db-afdd-41a3-b338-dc5f34417acb|2024-01-04            |66e48fb4-3ea7-48ae-a675-83f636d17fa8-0_1-134-689_20240618113730201.parquet|e50b97db-afdd-41a3-b338-dc5f34417acb|David|42 |null|2024-01-04|
# |20240618112155544  |20240618112155544_2_0|242a244d-bac4-4784-8fc3-6c880411accf|2024-01-03            |30ce8c3c-1909-4849-8084-b5401eb2d637-0_2-62-479_20240618112155544.parquet |242a244d-bac4-4784-8fc3-6c880411accf|Cathy|29 |null|2024-01-03|
# |20240618112155544  |20240618112155544_0_0|316cef8b-d889-4246-b65e-caf8fd10f930|2024-01-02            |3e8b5791-094e-4eb9-a738-440c2a2b712f-0_0-62-477_20240618112155544.parquet |316cef8b-d889-4246-b65e-caf8fd10f930|Bob  |45 |null|2024-01-02|
# +-------------------+---------------------+------------------------------------+----------------------+--------------------------------------------------------------------------+------------------------------------+-----+---+----+----------+

# 削除

# 追加するデータの作成（更新データ）
del_data = [
    (df.first()["uuid"], "Alice", 35, "2024-01-01", "hoge")  # Aliceのデータを削除
]


columns2 = ["uuid", "name", "age", "ts", "add"]
delete_df = spark.createDataFrame(del_data, columns2)

delete_df.write.format("hudi") \
    .option("hoodie.table.name", "hudi_test_table") \
    .option("hoodie.datasource.write.recordkey.field", "uuid") \
    .option("hoodie.datasource.write.partitionpath.field", "ts") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .option("hoodie.datasource.write.operation", "delete") \
    .mode("append") \
    .save(hudi_table_path)

result = spark.read.format("hudi").option("hoodie.datasource.query.type", "snapshot").load(hudi_table_path)

# >>> result.show(truncate=False)
# +-------------------+---------------------+------------------------------------+----------------------+--------------------------------------------------------------------------+------------------------------------+-----+---+----+----------+
# |_hoodie_commit_time|_hoodie_commit_seqno |_hoodie_record_key                  |_hoodie_partition_path|_hoodie_file_name                                                         |uuid                                |name |age|add |ts        |
# +-------------------+---------------------+------------------------------------+----------------------+--------------------------------------------------------------------------+------------------------------------+-----+---+----+----------+
# |20240618113730201  |20240618113730201_1_0|e50b97db-afdd-41a3-b338-dc5f34417acb|2024-01-04            |66e48fb4-3ea7-48ae-a675-83f636d17fa8-0_1-134-689_20240618113730201.parquet|e50b97db-afdd-41a3-b338-dc5f34417acb|David|42 |null|2024-01-04|
# |20240618112155544  |20240618112155544_2_0|242a244d-bac4-4784-8fc3-6c880411accf|2024-01-03            |30ce8c3c-1909-4849-8084-b5401eb2d637-0_2-62-479_20240618112155544.parquet |242a244d-bac4-4784-8fc3-6c880411accf|Cathy|29 |null|2024-01-03|
# |20240618112155544  |20240618112155544_0_0|316cef8b-d889-4246-b65e-caf8fd10f930|2024-01-02            |3e8b5791-094e-4eb9-a738-440c2a2b712f-0_0-62-477_20240618112155544.parquet |316cef8b-d889-4246-b65e-caf8fd10f930|Bob  |45 |null|2024-01-02|
# +-------------------+---------------------+------------------------------------+----------------------+--------------------------------------------------------------------------+------------------------------------+-----+---+----+----------+


# タイムトラベル
time_travel = spark.read.format("hudi") \
    .option("as.of.instant", "20240618112155544") \
    .load(hudi_table_path)

# time_travelデータ
# >>> # time_travel.show(truncate=False)
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+
# |_hoodie_commit_time|_hoodie_commit_seqno |_hoodie_record_key                  |_hoodie_partition_path|_hoodie_file_name                                                        |uuid                                |name |age|ts        |
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+
# |20240618112155544  |20240618112155544_2_0|242a244d-bac4-4784-8fc3-6c880411accf|2024-01-03            |30ce8c3c-1909-4849-8084-b5401eb2d637-0_2-62-479_20240618112155544.parquet|242a244d-bac4-4784-8fc3-6c880411accf|Cathy|29 |2024-01-03|
# |20240618112155544  |20240618112155544_1_0|97ad882b-1de7-4393-acc1-f9c09fed7336|2024-01-01            |de1c4eee-4164-4b32-b430-65c891568bc1-0_1-62-478_20240618112155544.parquet|97ad882b-1de7-4393-acc1-f9c09fed7336|Alice|34 |2024-01-01|
# |20240618112155544  |20240618112155544_0_0|316cef8b-d889-4246-b65e-caf8fd10f930|2024-01-02            |3e8b5791-094e-4eb9-a738-440c2a2b712f-0_0-62-477_20240618112155544.parquet|316cef8b-d889-4246-b65e-caf8fd10f930|Bob  |45 |2024-01-02|
# +-------------------+---------------------+------------------------------------+----------------------+-------------------------------------------------------------------------+------------------------------------+-----+---+----------+

time_travel = spark.read.format("hudi") \
    .option("as.of.instant", "20240618114348498") \
    .load(hudi_table_path)

# time_travelデータ
# >>> # time_travel.show(truncate=False)
# +-------------------+---------------------+------------------------------------+----------------------+--------------------------------------------------------------------------+------------------------------------+-----+---+----+----------+
# |_hoodie_commit_time|_hoodie_commit_seqno |_hoodie_record_key                  |_hoodie_partition_path|_hoodie_file_name                                                         |uuid                                |name |age|add |ts        |
# +-------------------+---------------------+------------------------------------+----------------------+--------------------------------------------------------------------------+------------------------------------+-----+---+----+----------+
# |20240618114348496  |20240618114348496_0_0|97ad882b-1de7-4393-acc1-f9c09fed7336|2024-01-01            |de1c4eee-4164-4b32-b430-65c891568bc1-0_0-173-800_20240618114348496.parquet|97ad882b-1de7-4393-acc1-f9c09fed7336|Alice|35 |hoge|2024-01-01|
# |20240618113730201  |20240618113730201_1_0|e50b97db-afdd-41a3-b338-dc5f34417acb|2024-01-04            |66e48fb4-3ea7-48ae-a675-83f636d17fa8-0_1-134-689_20240618113730201.parquet|e50b97db-afdd-41a3-b338-dc5f34417acb|David|42 |null|2024-01-04|
# |20240618112155544  |20240618112155544_2_0|242a244d-bac4-4784-8fc3-6c880411accf|2024-01-03            |30ce8c3c-1909-4849-8084-b5401eb2d637-0_2-62-479_20240618112155544.parquet |242a244d-bac4-4784-8fc3-6c880411accf|Cathy|29 |null|2024-01-03|
# |20240618112155544  |20240618112155544_0_0|316cef8b-d889-4246-b65e-caf8fd10f930|2024-01-02            |3e8b5791-094e-4eb9-a738-440c2a2b712f-0_0-62-477_20240618112155544.parquet |316cef8b-d889-4246-b65e-caf8fd10f930|Bob  |45 |null|2024-01-02|
# +-------------------+---------------------+------------------------------------+----------------------+--------------------------------------------------------------------------+------------------------------------+-----+---+----+----------+