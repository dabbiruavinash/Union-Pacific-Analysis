from pyspark.sql import DataFrame
from config.settings import FileFormats

class FileFormatOptimizer:
    def __init__(self, spark):
        self.spark = spark
    
    def optimize_file_format(self, df: DataFrame, output_path: str, 
                           format_type: FileFormats, compression: str = "snappy") -> None:
        """Optimize file format and compression"""
        writer = df.write.mode("overwrite")
        
        if format_type == FileFormats.PARQUET:
            writer.option("compression", compression) \
                 .option("parquet.block.size", "134217728") \  # 128MB
                 .parquet(output_path)
        
        elif format_type == FileFormats.ORC:
            writer.option("compression", compression) \
                 .orc(output_path)
        
        elif format_type == FileFormats.AVRO:
            writer.option("compression", compression) \
                 .format("avro").save(output_path)
    
    def compare_formats(self, df: DataFrame, test_path: str) -> Dict:
        """Compare different file formats"""
        formats = [FileFormats.PARQUET, FileFormats.ORC, FileFormats.AVRO]
        results = {}
        
        for format_type in formats:
            test_output = f"{test_path}/{format_type.value}"
            
            start_time = time.time()
            self.optimize_file_format(df, test_output, format_type)
            write_time = time.time() - start_time
            
            start_time = time.time()
            test_df = self.spark.read.format(format_type.value).load(test_output)
            test_df.count()
            read_time = time.time() - start_time
            
            file_size = self._get_directory_size(test_output)
            
            results[format_type.value] = {
                "write_time": write_time,
                "read_time": read_time,
                "file_size_mb": file_size / (1024 * 1024)
            }
        
        return results