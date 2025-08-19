from pyspark.sql import DataFrame
import time
from functools import wraps

class PerformanceMonitor:
    def __init__(self, spark):
        self.spark = spark
    
    def time_function(self, func):
        """Decorator to time function execution"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            print(f"Function {func.__name__} took {end_time - start_time:.2f} seconds")
            return result
        return wrapper
    
    def analyze_query_plan(self, df: DataFrame) -> None:
        """Analyze and print query plan"""
        print("=== Logical Plan ===")
        print(df._jdf.queryExecution().logical().toString())
        
        print("\n=== Optimized Plan ===")
        print(df._jdf.queryExecution().optimizedPlan().toString())
        
        print("\n=== Physical Plan ===")
        print(df._jdf.queryExecution().executedPlan().toString())
    
    def check_aqe_status(self) -> None:
        """Check Adaptive Query Execution status"""
        conf = self.spark.sparkContext.getConf()
        aqe_enabled = conf.get("spark.sql.adaptive.enabled", "false")
        print(f"Adaptive Query Execution enabled: {aqe_enabled}")