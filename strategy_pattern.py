from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import List

# ===== STRATEGY PATTERN =====
class JoinStrategy(ABC):
    """Strategy interface for different join approaches"""
    
    @abstractmethod
    def execute_join(self, left_df: DataFrame, right_df: DataFrame, 
                    join_keys: List[str]) -> DataFrame:
        pass

class BroadcastJoinStrategy(JoinStrategy):
    """Concrete strategy for broadcast joins"""
    
    def execute_join(self, left_df: DataFrame, right_df: DataFrame, 
                    join_keys: List[str]) -> DataFrame:
        from pyspark.sql.functions import broadcast
        return left_df.join(broadcast(right_df), join_keys)

class SortMergeJoinStrategy(JoinStrategy):
    """Concrete strategy for sort-merge joins"""
    
    def execute_join(self, left_df: DataFrame, right_df: DataFrame, 
                    join_keys: List[str]) -> DataFrame:
        return left_df.join(right_df.hint("merge"), join_keys)

class SkewJoinStrategy(JoinStrategy):
    """Concrete strategy for handling skewed joins"""
    
    def __init__(self, salt_buckets: int = 10):
        self.salt_buckets = salt_buckets
    
    def execute_join(self, left_df: DataFrame, right_df: DataFrame, 
                    join_keys: List[str]) -> DataFrame:
        from pyspark.sql import functions as F
        
        # Add salt to handle skew
        left_salted = left_df.withColumn(
            "salted_key", 
            F.concat(F.col(join_keys[0]), F.lit("_"), 
                    (F.rand() * self.salt_buckets).cast("int"))
        )
        
        right_salted = right_df.withColumn(
            "salted_key", 
            F.concat(F.col(join_keys[0]), F.lit("_"), 
                    (F.rand() * self.salt_buckets).cast("int"))
        )
        
        return left_salted.join(right_salted, "salted_key")

# ===== CONTEXT CLASS =====
class JoinOptimizer:
    """Context class that uses strategy pattern"""
    
    def __init__(self, strategy: JoinStrategy = None):
        self._strategy = strategy
    
    def set_strategy(self, strategy: JoinStrategy):
        """Set the join strategy dynamically"""
        self._strategy = strategy
    
    def execute_join(self, left_df: DataFrame, right_df: DataFrame, 
                    join_keys: List[str]) -> DataFrame:
        """Execute join using current strategy"""
        if not self._strategy:
            raise ValueError("No join strategy set")
        
        return self._strategy.execute_join(left_df, right_df, join_keys)