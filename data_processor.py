from pyspark.sql import DataFrame, functions as F
from pyspark import SparkContext
from utils.accumulator_utils import CustomAccumulator
from utils.broadcast_utils import BroadcastManager

class DataProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.sc = spark.sparkContext
        self.broadcast_manager = BroadcastManager(spark)
    
    def handle_skew_data(self, df: DataFrame, key_column: str, salt_buckets: int = 10) -> DataFrame:
        """Skew data handling using salting technique"""
        return df.withColumn("salted_key", 
                           F.concat(F.col(key_column), F.lit("_"), 
                                   (F.rand() * salt_buckets).cast("int")))
    
    def optimize_joins(self, left_df: DataFrame, right_df: DataFrame, 
                      join_key: str, join_type: str = "inner") -> DataFrame:
        """Join optimization with broadcast hints"""
        # Broadcast smaller table if applicable
        right_df_size = right_df.rdd.getNumPartitions() * right_df.rdd.mapPartitions(lambda x: [sum(1 for _ in x)]).sum()
        
        if right_df_size < 10 * 1024 * 1024:  # 10MB threshold
            right_df = self.broadcast_manager.broadcast_df(right_df)
        
        return left_df.join(right_df, join_key, join_type)
    
    def repartition_data(self, df: DataFrame, partition_strategy: str, 
                        columns: list = None, num_partitions: int = None) -> DataFrame:
        """Advanced repartitioning strategies"""
        if partition_strategy == "hash":
            return df.repartition(num_partitions, *columns)
        elif partition_strategy == "range":
            return df.repartitionByRange(num_partitions, *columns)
        elif partition_strategy == "bucket":
            return df.write.bucketBy(num_partitions, columns[0]).saveAsTable("bucketed_table")
        else:
            return df.coalesce(num_partitions)