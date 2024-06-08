from identity_uuid_mapping import IdentityUUIDMapping
from pyspark.sql import SparkSession

def main(data_sources):
    # Initialize Spark session
    spark = SparkSession.builder.appName("IdentityUUIDMapping").getOrCreate()
    
    # UUID Map table name
    uuid_map_table_name = "database.schema.gold__identity_uuid_map"
    
    # Process data
    identity_uuid_mapping = IdentityUUIDMapping(spark, uuid_map_table_name)
    identity_uuid_mapping.process_data(data_sources)
    
if __name__ == "__main__":
    data_sources = [         
        {"type": "csv", "path": "path/to/customer_data_crm.csv", "phone_col": "phone_identifier", "email_col": "email_identifier", "id_col": "unique_record_id"},
        {"type": "csv", "path": "path/to/customer_data_salesforce_export.csv", "phone_col": "phone_identifier", "email_col": "email_identifier", "id_col": "unique_record_id"},
        {"type": "delta", "path": "path/to/customer_data_shopify", "phone_col": "phone_identifier", "email_col": "email_identifier", "id_col": "id"}
    ]
    main(data_sources)