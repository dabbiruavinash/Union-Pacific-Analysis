from pyspark.sql import DataFrame
import time
from functools import wraps
from memory_profiler import memory_usage

class AdvancedPerformanceMonitor:
    def __init__(self, spark):
        self.spark = spark
        self.metrics = {}
    
    def monitor_memory_usage(self, func):
        """Monitor memory usage of function"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            mem_usage = memory_usage((func, args, kwargs))
            self.metrics[func.__name__] = {
                "max_memory_mb": max(mem_usage),
                "avg_memory_mb": sum(mem_usage) / len(mem_usage)
            }
            return func(*args, **kwargs)
        return wrapper
    
    def analyze_join_performance(self, left_df: DataFrame, right_df: DataFrame, 
                               join_condition: str) -> Dict:
        """Analyze join performance characteristics"""
        # Analyze data characteristics
        left_size = left_df.rdd.getNumPartitions()
        right_size = right_df.rdd.getNumPartitions()
        
        # Test different join strategies
        strategies = ["broadcast", "sort_merge", "shuffle_hash"]
        results = {}
        
        for strategy in strategies:
            start_time = time.time()
            
            if strategy == "broadcast":
                result = left_df.join(right_df.hint("broadcast"), join_condition)
            elif strategy == "sort_merge":
                result = left_df.join(right_df.hint("merge"), join_condition)
            else:
                result = left_df.join(right_df, join_condition)
            
            result.count()  Force execution
            execution_time = time.time() - start_time
            
            results[strategy] = {
                "execution_time": execution_time,
                "partitions": result.rdd.getNumPartitions()
            }
        
        return results