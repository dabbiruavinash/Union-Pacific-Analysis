from typing import List, Dict, Any
from pyspark.sql import DataFrame
from core.oop_foundation import DataProcessor

# ===== BUILDER PATTERN =====
class DataPipelineBuilder:
    """Builder for complex data processing pipelines"""
    
    def __init__(self):
        self.processors: List[DataProcessor] = []
        self.validation_rules: Dict[str, Any] = {}
        self.performance_optimizations: List[str] = []
    
    def add_processor(self, processor: DataProcessor) -> 'DataPipelineBuilder':
        self.processors.append(processor)
        return self
    
    def add_validation_rule(self, column: str, rule: Dict[str, Any]) -> 'DataPipelineBuilder':
        self.validation_rules[column] = rule
        return self
    
    def enable_performance_optimization(self, optimization: str) -> 'DataPipelineBuilder':
        self.performance_optimizations.append(optimization)
        return self
    
    def build(self) -> 'DataPipeline':
        return DataPipeline(
            processors=self.processors,
            validation_rules=self.validation_rules,
            performance_optimizations=self.performance_optimizations
        )

class DataPipeline:
    """Complex object built by the builder"""
    
    def __init__(self, processors: List[DataProcessor], 
                 validation_rules: Dict[str, Any], 
                 performance_optimizations: List[str]):
        self.processors = processors
        self.validation_rules = validation_rules
        self.performance_optimizations = performance_optimizations
    
    def execute(self, df: DataFrame) -> DataFrame:
        result = df
        for processor in self.processors:
            result = processor.process_data(result)
        return result
    
    def get_configuration(self) -> Dict[str, Any]:
        return {
            "processor_count": len(self.processors),
            "validation_rules": self.validation_rules,
            "performance_optimizations": self.performance_optimizations
        }