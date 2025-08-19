from pyspark.sql import DataFrame
from datetime import datetime, timedelta

class PartitionManager:
    def __init__(self, spark):
        self.spark = spark
    
    def manage_partition_evolution(self, df: DataFrame, partition_columns: List[str], 
                                 new_partition_strategy: Dict) -> DataFrame:
        """Manage partition evolution and migration"""
        current_partitions = self._get_current_partitions(df)
        
        if set(partition_columns) != set(current_partitions):
            # Partition strategy changed, need to migrate
            return self._migrate_partitions(df, current_partitions, partition_columns)
        
        return df
    
    def optimize_partition_size(self, df: DataFrame, target_size_mb: int = 128) -> DataFrame:
        """Optimize partition sizes for better performance"""
        total_size_mb = self._estimate_dataframe_size(df)
        optimal_partitions = max(1, int(total_size_mb / target_size_mb))
        
        return df.repartition(optimal_partitions)
    
    def handle_date_partitions(self, df: DataFrame, date_column: str, 
                             retention_days: int = 365) -> DataFrame:
        """Manage date-based partitions with retention"""
        current_date = datetime.now()
        cutoff_date = current_date - timedelta(days=retention_days)
        
        # Filter out old partitions
        return df.filter(F.col(date_column) >= cutoff_date)