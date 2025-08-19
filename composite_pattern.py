from abc import ABC, abstractmethod
from typing import List
from pyspark.sql import DataFrame

# ===== COMPOSITE PATTERN =====
class DataProcessingComponent(ABC):
    """Component interface for composite pattern"""
    
    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        pass

class DataProcessingLeaf(DataProcessingComponent):
    """Leaf node representing individual processor"""
    
    def __init__(self, name: str, processor: callable):
        self.name = name
        self.processor = processor
    
    def process(self, df: DataFrame) -> DataFrame:
        return self.processor(df)
    
    def get_name(self) -> str:
        return self.name

class DataProcessingComposite(DataProcessingComponent):
    """Composite node that contains other components"""
    
    def __init__(self, name: str):
        self.name = name
        self.children: List[DataProcessingComponent] = []
    
    def add(self, component: DataProcessingComponent):
        self.children.append(component)
    
    def remove(self, component: DataProcessingComponent):
        self.children.remove(component)
    
    def process(self, df: DataFrame) -> DataFrame:
        result = df
        for child in self.children:
            result = child.process(result)
        return result
    
    def get_name(self) -> str:
        return self.name
    
    def get_children(self) -> List[DataProcessingComponent]:
        return self.children.copy()