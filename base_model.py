from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseModel(ABC):
    def __init__(self, spark):
        self.spark = spark
        self.sc = spark.sparkContext
    
    @abstractmethod
    def train(self, data: DataFrame) -> None:
        pass
    
    @abstractmethod
    def predict(self, data: DataFrame) -> DataFrame:
        pass
    
    @abstractmethod
    def save_model(self, path: str) -> None:
        pass
    
    @abstractmethod
    def load_model(self, path: str) -> None:
        pass