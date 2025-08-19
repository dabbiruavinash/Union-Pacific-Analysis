from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel

class CacheManager:
    def __init__(self, spark):
        self.spark = spark
        self.cached_dfs = {}
    
    def cache_dataframe(self, df: DataFrame, storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK) -> DataFrame:
        """Cache DataFrame with specified storage level"""
        cached_df = df.persist(storage_level)
        self.cached_dfs[df] = cached_df
        return cached_df
    
    def uncache_dataframe(self, df: DataFrame) -> None:
        """Uncache DataFrame"""
        if df in self.cached_dfs:
            self.cached_dfs[df].unpersist()
            del self.cached_dfs[df]
    
    def clear_all_cache(self) -> None:
        """Clear all cached DataFrames"""
        for df in list(self.cached_dfs.keys()):
            self.uncache_dataframe(df)
    
    def optimize_cache_strategy(self, df: DataFrame, access_pattern: str) -> StorageLevel:
        """Choose optimal cache strategy based on access pattern"""
        if access_pattern == "frequent_read":
            return StorageLevel.MEMORY_ONLY
        elif access_pattern == "infrequent_read":
            return StorageLevel.MEMORY_AND_DISK
        elif access_pattern == "disk_only":
            return StorageLevel.DISK_ONLY
        else:
            return StorageLevel.MEMORY_AND_DISK