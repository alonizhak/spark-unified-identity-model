from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class DataLoader:
    def __init__(self, spark):
        self.spark = spark

    def load_data_from_csv(self, path):
        return self.spark.read.csv(path, header=True)

    def load_data_from_delta(self, path):
        return self.spark.read.format("delta").load(path)

    def load_all_data(self, data_sources):
        dataframes = []
        for source in data_sources:
            if source["type"] == "csv":
                df = self.load_data_from_csv(source["path"])
            elif source["type"] == "delta":
                df = self.load_data_from_delta(source["path"])
            df = df.select(source["phone_col"], source["email_col"], col(source["id_col"]).alias("unique_record_id"))
            dataframes.append(df)
        return dataframes