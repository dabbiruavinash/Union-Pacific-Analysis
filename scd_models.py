from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from datetime import datetime

class SCDModel:
    def __init__(self, spark):
        self.spark = spark
    
    def create_scd2_table(self, initial_data: DataFrame, key_columns: List[str]) -> DataFrame:
        """Create initial SCD2 table with versioning"""
        return initial_data.withColumn("effective_date", F.current_date()) \
                         .withColumn("expiry_date", F.lit(None).cast("date")) \
                         .withColumn("is_current", F.lit(True)) \
                         .withColumn("version", F.lit(1))
    
    def update_scd2(self, current_table: DataFrame, updates: DataFrame, 
                   key_columns: List[str]) -> DataFrame:
        """Update SCD2 table with new records"""
        # Expire current records that have updates
        updates_keys = updates.select(*key_columns).distinct()
        
        expired_records = current_table.join(updates_keys, key_columns) \
            .filter(F.col("is_current") == True) \
            .withColumn("expiry_date", F.current_date()) \
            .withColumn("is_current", F.lit(False))
        
        # Add new versions
        new_versions = updates.withColumn("effective_date", F.current_date()) \
                            .withColumn("expiry_date", F.lit(None).cast("date")) \
                            .withColumn("is_current", F.lit(True)) \
                            .withColumn("version", F.lit(1))
        
        # Combine all records
        return expired_records.unionByName(new_versions)