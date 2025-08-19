from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from typing import List, Dict

class SkewHandler:
    def __init__(self, spark):
        self.spark = spark
    
    def detect_skew(self, df: DataFrame, key_columns: List[str], threshold: float = 0.8) -> Dict:
        """Detect data skew in DataFrame"""
        skew_analysis = {}
        
        for col in key_columns:
            stats = df.groupBy(col).count().agg(
                F.mean("count").alias("mean"),
                F.stddev("count").alias("stddev"),
                F.max("count").alias("max")
            ).first()
            
            skew_ratio = stats["max"] / stats["mean"] if stats["mean"] > 0 else 0
            is_skewed = skew_ratio > threshold
            
            skew_analysis[col] = {
                "skew_ratio": skew_ratio,
                "is_skewed": is_skewed,
                "max_count": stats["max"],
                "mean_count": stats["mean"]
            }
        
        return skew_analysis
    
    def handle_skew_with_salting(self, df: DataFrame, key_column: str, num_salts: int = 10) -> DataFrame:
        """Handle skew using salting technique"""
        return df.withColumn("salted_key", 
                           F.concat(F.col(key_column), F.lit("_"), 
                                   (F.rand() * num_salts).cast("int")))
    
    def handle_skew_with_repartition(self, df: DataFrame, key_column: str, 
                                   target_partitions: int = 200) -> DataFrame:
        """Handle skew using custom repartitioning"""
        # Analyze key distribution
        key_distribution = df.groupBy(key_column).count().cache()
        
        # Identify skewed keys
        avg_count = key_distribution.agg(F.avg("count")).first()[0]
        skewed_keys = key_distribution.filter(F.col("count") > 2 * avg_count).collect()
        
        # Create custom partitioner
        if skewed_keys:
            # For skewed keys, assign multiple partitions
            skewed_mapping = {}
            for row in skewed_keys:
                key = row[key_column]
                count = row["count"]
                partitions_needed = min(int(count / avg_count) + 1, 10)
                skewed_mapping[key] = partitions_needed
            
            # Add partition hint column
            def assign_partition(key, count):
                if key in skewed_mapping:
                    return hash(key) % (skewed_mapping[key] * 10)
                return hash(key) % target_partitions
            
            assign_udf = F.udf(assign_partition, IntegerType())
            df = df.withColumn("partition_id", 
                             assign_udf(F.col(key_column), F.lit(1)))
            
            return df.repartition(target_partitions, "partition_id")
        
        return df.repartition(target_partitions, key_column)