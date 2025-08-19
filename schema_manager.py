from pyspark.sql.types import *
from pyspark.sql import DataFrame

class SchemaManager:
    def __init__(self, spark):
        self.spark = spark
    
    def enforce_schema(self, df: DataFrame, expected_schema: StructType) -> DataFrame:
        """Schema enforcement with evolution support"""
        current_schema = df.schema
        
        # Check for missing columns
        missing_columns = [field.name for field in expected_schema 
                         if field.name not in [f.name for f in current_schema]]
        
        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")
        
        # Cast columns to expected types
        for field in expected_schema:
            if field.name in df.columns:
                df = df.withColumn(field.name, df[field.name].cast(field.dataType))
        
        return df
    
    def evolve_schema(self, df: DataFrame, new_schema: StructType) -> DataFrame:
        """Schema evolution handling"""
        current_fields = {f.name: f for f in df.schema}
        
        for field in new_schema:
            if field.name not in current_fields:
                # Add new column with default value
                df = df.withColumn(field.name, lit(None).cast(field.dataType))
        
        return df.select([field.name for field in new_schema])