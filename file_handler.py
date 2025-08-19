from pyspark.sql import DataFrame
from config.settings import FileFormats, DataPaths

class FileHandler:
    def __init__(self, spark):
        self.spark = spark
    
    def read_file(self, path: str, file_format: FileFormats, **options) -> DataFrame:
        """Read different file formats with predicate pushdown"""
        reader = self.spark.read
        
        if file_format == FileFormats.CSV:
            return reader.option("header", "true") \
                       .option("inferSchema", "true") \
                       .csv(path, **options)
        
        elif file_format == FileFormats.PARQUET:
            return reader.parquet(path, **options)
        
        elif file_format == FileFormats.JSON:
            return reader.json(path, **options)
        
        elif file_format == FileFormats.ORC:
            return reader.orc(path, **options)
    
    def write_file(self, df: DataFrame, path: str, file_format: FileFormats, 
                  mode: str = "overwrite", partition_by: list = None, 
                  num_partitions: int = None) -> None:
        """Write data with partitioning and compression"""
        if num_partitions:
            df = df.coalesce(num_partitions)
        
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        if file_format == FileFormats.PARQUET:
            writer.parquet(path)
        elif file_format == FileFormats.CSV:
            writer.option("header", "true").csv(path)
        elif file_format == FileFormats.JSON:
            writer.json(path)