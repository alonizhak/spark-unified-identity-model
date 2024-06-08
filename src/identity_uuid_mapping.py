from uuid_map_manager import UUIDMapManager
from data_loader import DataLoader
from connected_components_finder import ConnectedComponentsFinder
from graphframes_connected_components_finder import GraphFramesConnectedComponentsFinder
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

class IdentityUUIDMapping:
    def __init__(self, spark, uuid_map_table_name):
        self.spark = spark
        self.uuid_map_manager = UUIDMapManager(spark, uuid_map_table_name)
        self.data_loader = DataLoader(spark)

    def process_data(self, data_sources):
        # Load all data sources
        dataframes = self.data_loader.load_all_data(data_sources)
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.union(df)

        data = combined_df.rdd.map(lambda row: (row["unique_record_id"], {"phone": row["phone_identifier"], "email": row["email_identifier"]})).collectAsMap()

        previous_uuid_map = self.uuid_map_manager.load_existing_uuid_map()

        if len(data) > 10000000:  # Arbitrary threshold for large datasets
            logging.info("Using GraphFrames for connected components")
            cc_finder = GraphFramesConnectedComponentsFinder(self.spark)
            result_df = cc_finder.find_connected_components(data)
        else:
            logging.info("Using optimized union-find for connected components")
            cc_finder = ConnectedComponentsFinder(previous_uuid_map)
            result, uuid_map = cc_finder.find_connected_components(data)
            self.uuid_map_manager.save_uuid_map(uuid_map)
            result_df = self.spark.createDataFrame(result)
        
        result_df = result_df.filter((col("email").isNotNull()) | (col("phone").isNotNull()))
        result_df = result_df.select("email", "phone", "uuid").distinct()
        result_df.write.mode("overwrite").format("delta").save("path/to/output/delta")