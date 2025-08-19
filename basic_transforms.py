from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import *

class BasicTransformations:
    @staticmethod
    def map_transformation(df: DataFrame, map_func) -> DataFrame:
        """Map transformation example"""
        return df.rdd.map(map_func).toDF()
    
    @staticmethod
    def flatmap_transformation(df: DataFrame, flatmap_func) -> DataFrame:
        """FlatMap transformation example"""
        return df.rdd.flatMap(flatmap_func).toDF()
    
    @staticmethod
    def narrow_transformations(df: DataFrame) -> DataFrame:
        """Narrow transformations"""
        return df.filter(F.col("status") == "active") \
                .select("train_id", "status") \
                .withColumn("processed_time", F.current_timestamp())
    
    @staticmethod
    def wide_transformations(df: DataFrame) -> DataFrame:
        """Wide transformations with partitioning"""
        return df.groupBy("origin", "destination") \
                .agg(F.avg("current_speed").alias("avg_speed"),
                     F.count("train_id").alias("train_count")) \
                .repartition(10, "origin")