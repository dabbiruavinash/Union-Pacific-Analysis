from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Callable, Any
import time
from functools import wraps

# ===== DECORATOR PATTERN =====
class DataProcessorDecorator(DataProcessor):
    """Base decorator class"""
    
    def __init__(self, processor: DataProcessor):
        self._processor = processor
    
    def process_data(self, df: DataFrame) -> DataFrame:
        return self._processor.process_data(df)
    
    def validate_data(self, df: DataFrame) -> bool:
        return self._processor.validate_data(df)

class TimingDecorator(DataProcessorDecorator):
    """Decorator to add timing functionality"""
    
    def process_data(self, df: DataFrame) -> DataFrame:
        start_time = time.time()
        result = super().process_data(df)
        end_time = time.time()
        
        print(f"Processing took {end_time - start_time:.2f} seconds")
        return result

class LoggingDecorator(DataProcessorDecorator):
    """Decorator to add logging functionality"""
    
    def __init__(self, processor: DataProcessor, logger_name: str = "DataProcessor"):
        super().__init__(processor)
        self.logger_name = logger_name
    
    def process_data(self, df: DataFrame) -> DataFrame:
        print(f"[{self.logger_name}] Starting data processing...")
        result = super().process_data(df)
        print(f"[{self.logger_name}] Data processing completed")
        return result

class CachingDecorator(DataProcessorDecorator):
    """Decorator to add caching functionality"""
    
    def __init__(self, processor: DataProcessor, storage_level: str = "MEMORY_AND_DISK"):
        super().__init__(processor)
        self.storage_level = storage_level
        self._cached_result = None
    
    def process_data(self, df: DataFrame) -> DataFrame:
        if self._cached_result is None:
            self._cached_result = super().process_data(df).persist(self.storage_level)
        return self._cached_result

# ===== FUNCTION DECORATORS =====
def measure_performance(func: Callable) -> Callable:
    """Function decorator for performance measurement"""
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        print(f"{func.__name__} executed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper

def validate_input(schema: List[str]):
    """Decorator factory for input validation"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, df: DataFrame, *args, **kwargs) -> Any:
            missing_columns = [col for col in schema if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            return func(self, df, *args, **kwargs)
        return wrapper
    return decorator