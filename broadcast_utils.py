from pyspark.sql import DataFrame
from pyspark.broadcast import Broadcast

class BroadcastManager:
    def __init__(self, spark):
        self.spark = spark
        self.sc = spark.sparkContext
        self.broadcast_vars = {}
    
    def broadcast_df(self, df: DataFrame) -> DataFrame:
        """Broadcast DataFrame for join optimization"""
        broadcast_df = self.sc.broadcast(df.collect())
        self.broadcast_vars[df] = broadcast_df
        return df
    
    def cleanup_broadcast(self):
        """Clean up broadcast variables"""
        for var in self.broadcast_vars.values():
            var.unpersist()
        self.broadcast_vars.clear()