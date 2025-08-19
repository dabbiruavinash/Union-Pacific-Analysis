from abc import ABC, abstractmethod
from typing import List, Dict, Any
from pyspark.sql import DataFrame

# ===== OBSERVER PATTERN =====
class DataProcessingObserver(ABC):
    """Observer interface for data processing events"""
    
    @abstractmethod
    def on_processing_start(self, df: DataFrame, processor: str):
        pass
    
    @abstractmethod
    def on_processing_complete(self, df: DataFrame, processor: str, stats: Dict[str, Any]):
        pass
    
    @abstractmethod
    def on_processing_error(self, df: DataFrame, processor: str, error: Exception):
        pass

class LoggingObserver(DataProcessingObserver):
    """Concrete observer for logging events"""
    
    def on_processing_start(self, df: DataFrame, processor: str):
        print(f"[LOG] Processing started: {processor}, rows: {df.count()}")
    
    def on_processing_complete(self, df: DataFrame, processor: str, stats: Dict[str, Any]):
        print(f"[LOG] Processing completed: {processor}, result rows: {df.count()}")
    
    def on_processing_error(self, df: DataFrame, processor: str, error: Exception):
        print(f"[ERROR] Processing failed: {processor}, error: {error}")

class MetricsObserver(DataProcessingObserver):
    """Concrete observer for collecting metrics"""
    
    def __init__(self):
        self.metrics = {}
    
    def on_processing_start(self, df: DataFrame, processor: str):
        self.metrics[processor] = {"start_time": time.time(), "input_rows": df.count()}
    
    def on_processing_complete(self, df: DataFrame, processor: str, stats: Dict[str, Any]):
        if processor in self.metrics:
            self.metrics[processor].update({
                "end_time": time.time(),
                "output_rows": df.count(),
                "processing_time": time.time() - self.metrics[processor]["start_time"]
            })
    
    def on_processing_error(self, df: DataFrame, processor: str, error: Exception):
        if processor in self.metrics:
            self.metrics[processor]["error"] = str(error)

# ===== SUBJECT CLASS =====
class ObservableDataProcessor(DataProcessor):
    """Subject class that notifies observers"""
    
    def __init__(self):
        self._observers: List[DataProcessingObserver] = []
    
    def add_observer(self, observer: DataProcessingObserver):
        self._observers.append(observer)
    
    def remove_observer(self, observer: DataProcessingObserver):
        self._observers.remove(observer)
    
    def notify_processing_start(self, df: DataFrame, processor: str):
        for observer in self._observers:
            observer.on_processing_start(df, processor)
    
    def notify_processing_complete(self, df: DataFrame, processor: str, stats: Dict[str, Any]):
        for observer in self._observers:
            observer.on_processing_complete(df, processor, stats)
    
    def notify_processing_error(self, df: DataFrame, processor: str, error: Exception):
        for observer in self._observers:
            observer.on_processing_error(df, processor, error)