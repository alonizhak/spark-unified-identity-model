from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, ArrayType, StructField
import logging
import json

class UUIDMapManager:
    def __init__(self, spark, uuid_map_table_name):
        self.spark = spark
        self.uuid_map_table_name = uuid_map_table_name

    def load_existing_uuid_map(self):
        try:
            df = self.spark.table(self.uuid_map_table_name)
            json_strings = df.toJSON().collect()
            existing_uuid_map_list = [json.loads(json_str) for json_str in json_strings]
            existing_uuid_map = {item["uuid"]: item["records"] for item in existing_uuid_map_list}
            logging.info(f"Table '{self.uuid_map_table_name}' successfully loaded as dictionary")
            return existing_uuid_map
        except Exception as e:
            logging.error(f"Table '{self.uuid_map_table_name}' does not exist. Exception: {e}")
            return {}

    def save_uuid_map(self, uuid_map):
        rows = [(uuid, records) for uuid, records in uuid_map.items()]
        schema = StructType([
            StructField("uuid", StringType(), True),
            StructField("records", ArrayType(StringType()), True)
        ])
        df = self.spark.createDataFrame(rows, schema)
        df.write.mode("overwrite").format("delta").saveAsTable(self.uuid_map_table_name)