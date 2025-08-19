from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import datetime

class VaccumManager:
    def __init__(self, spark):
        self.spark = spark
    
    def run_vaccum(self, table_path: str, retention_hours: int = 168) -> None:
        """Run vaccum on Delta table with retention policy"""
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        # Vaccum files older than retention period
        delta_table.vaccum(retentionHours=retention_hours)
        
        # Get vaccum statistics
        vaccum_stats = delta_table.history().filter("operation = 'VACCUM'").first()
        
        return {
            "files_deleted": vaccum_stats["operationMetrics"]["numDeletedFiles"],
            "files_retained": vaccum_stats["operationMetrics"]["numRetainedFiles"]
        }
    
    def optimize_table(self, table_path: str, zorder_columns: List[str] = None) -> None:
        """Optimize Delta table with Z-order"""
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        if zorder_columns:
            delta_table.optimize().executeZOrderBy(*zorder_columns)
        else:
            delta_table.optimize().executeCompaction()
        
        # Get optimization statistics
        optimize_stats = delta_table.history().filter("operation = 'OPTIMIZE'").first()
        
        return {
            "files_optimized": optimize_stats["operationMetrics"]["numFiles"],
            "partitions_optimized": optimize_stats["operationMetrics"]["numPartitions"]
        }