from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel
from typing import Dict

class CacheStrategyManager:
    def __init__(self, spark):
        self.spark = spark
        self.cache_strategies: Dict[str, StorageLevel] = {
            "frequent_read": StorageLevel.MEMORY_ONLY,
            "large_dataset": StorageLevel.MEMORY_AND_DISK,
            "archival": StorageLevel.DISK_ONLY,
            "computation_intensive": StorageLevel.MEMORY_ONLY_SER
        }
    
    def determine_optimal_strategy(self, df: DataFrame, usage_pattern: str) -> StorageLevel:
        """Determine optimal cache strategy based on usage pattern"""
        df_size = self._estimate_dataframe_size(df)
        
        if usage_pattern == "frequent_read" and df_size < 100 * 1024 * 1024:  # 100MB
            return StorageLevel.MEMORY_ONLY
        elif usage_pattern == "large_dataset" or df_size > 100 * 1024 * 1024:
            return StorageLevel.MEMORY_AND_DISK
        elif usage_pattern == "computation_intensive":
            return StorageLevel.MEMORY_ONLY_SER
        else:
            return StorageLevel.MEMORY_AND_DISK
    
    def manage_cache_lifecycle(self, df: DataFrame, strategy: StorageLevel, 
                             ttl_minutes: int = 60) -> None:
        """Manage cache lifecycle with time-to-live"""
        cached_df = df.persist(strategy)
        
        # Schedule automatic unpersist
        import threading
        def unpersist_later():
            import time
            time.sleep(ttl_minutes * 60)
            cached_df.unpersist()
        
        thread = threading.Thread(target=unpersist_later)
        thread.daemon = True
        thread.start()