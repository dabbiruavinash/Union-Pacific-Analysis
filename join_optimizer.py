from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class JoinOptimizer:
    def __init__(self, spark):
        self.spark = spark
    
    def optimize_broadcast_join(self, left_df: DataFrame, right_df: DataFrame, 
                               join_cols: list, join_type: str = "inner") -> DataFrame:
        """Optimize join using broadcast hint"""
        from pyspark.sql.functions import broadcast
        
        return left_df.join(broadcast(right_df), join_cols, join_type)
    
    def handle_skew_join(self, left_df: DataFrame, right_df: DataFrame, 
                        join_col: str, skew_threshold: int = 1000) -> DataFrame:
        """Handle skew in joins using salting technique"""
        # Analyze skew
        left_counts = left_df.groupBy(join_col).count()
        skewed_keys = left_counts.filter(F.col("count") > skew_threshold).select(join_col)
        
        if skewed_keys.count() > 0:
            # Add salt to skewed keys
            left_salted = left_df.join(skewed_keys, join_col) \
                .withColumn("salted_key", 
                          F.concat(F.col(join_col), F.lit("_"), F.floor(F.rand() * 10)))
            
            right_salted = right_df.crossJoin(
                self.spark.range(0, 10).withColumnRenamed("id", "salt")
            ).withColumn("salted_key", 
                       F.concat(F.col(join_col), F.lit("_"), F.col("salt")))
            
            return left_salted.join(right_salted, "salted_key")
        
        return left_df.join(right_df, join_col)