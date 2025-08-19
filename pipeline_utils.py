from pyspark.sql import DataFrame
from typing import List, Callable

class TransformationPipeline:
    def __init__(self):
        self.transformations: List[Callable] = []
    
    def add_transformation(self, transform_func: Callable) -> None:
        """Add transformation to pipeline"""
        self.transformations.append(transform_func)
    
    def execute_pipeline(self, df: DataFrame) -> DataFrame:
        """Execute all transformations in pipeline"""
        result = df
        for transform in self.transformations:
            result = transform(result)
        return result
    
    def optimize_pipeline(self) -> None:
        """Optimize transformation pipeline order"""
        # Reorder transformations for better performance
        # Move narrow transformations first, wide transformations later
        narrow_transforms = [t for t in self.transformations if self._is_narrow(t)]
        wide_transforms = [t for t in self.transformations if not self._is_narrow(t)]
        
        self.transformations = narrow_transforms + wide_transforms
    
    def _is_narrow(self, transform_func: Callable) -> bool:
        """Check if transformation is narrow"""
        # This would require analysis of the transformation function
        # For simplicity, we'll assume certain patterns
        transform_name = transform_func.__name__.lower()
        narrow_keywords = ["filter", "select", "withColumn", "drop"]
        return any(keyword in transform_name for keyword in narrow_keywords)