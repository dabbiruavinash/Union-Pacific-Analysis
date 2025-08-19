from pyspark.sql import DataFrame
from pyspark.broadcast import Broadcast
from typing import Dict, Any

class AdvancedBroadcastManager:
    def __init__(self, spark):
        self.spark = spark
        self.sc = spark.sparkContext
        self.broadcast_cache: Dict[str, Broadcast] = {}
    
    def broadcast_lookup_table(self, df: DataFrame, broadcast_name: str) -> None:
        """Broadcast DataFrame as lookup table"""
        collected_data = df.collect()
        broadcast_var = self.sc.broadcast({row[0]: row for row in collected_data})
        self.broadcast_cache[broadcast_name] = broadcast_var
        return broadcast_var
    
    def get_broadcasted_data(self, broadcast_name: str) -> Any:
        """Retrieve broadcasted data"""
        if broadcast_name in self.broadcast_cache:
            return self.broadcast_cache[broadcast_name].value
        return None
    
    def clear_broadcast_cache(self) -> None:
        """Clear all broadcast variables"""
        for name, var in self.broadcast_cache.items():
            var.unpersist()
        self.broadcast_cache.clear()