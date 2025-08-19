from abc import ABC, abstractmethod
from typing import List, Dict, Any
from pyspark.sql import DataFrame

# ===== ABSTRACTION =====
class DataProcessor(ABC):
    """Abstract base class demonstrating abstraction"""
    
    @abstractmethod
    def process_data(self, df: DataFrame) -> DataFrame:
        pass
    
    @abstractmethod
    def validate_data(self, df: DataFrame) -> bool:
        pass
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Concrete method with default implementation"""
        return {"processed_records": 0, "processing_time": 0}

# ===== INHERITANCE =====
class BaseDataValidator(DataProcessor):
    """Base class demonstrating inheritance"""
    
    def __init__(self):
        self.validation_rules = {}
        self.processed_count = 0
    
    def add_validation_rule(self, column: str, rule_type: str, **kwargs):
        """Add validation rule for specific column"""
        self.validation_rules[column] = {"type": rule_type, **kwargs}
    
    @abstractmethod
    def validate_data(self, df: DataFrame) -> bool:
        pass

# ===== ENCAPSULATION =====
class TrainDataValidator(BaseDataValidator):
    """Concrete class demonstrating encapsulation"""
    
    def __init__(self):
        super().__init__()
        self.__validation_errors = []  # Private attribute
        self._validation_passed = False  # Protected attribute
    
    def process_data(self, df: DataFrame) -> DataFrame:
        """Process data with validation"""
        if self.validate_data(df):
            processed_df = self._apply_business_rules(df)
            self.processed_count += processed_df.count()
            return processed_df
        raise ValueError("Data validation failed")
    
    def validate_data(self, df: DataFrame) -> bool:
        """Validate data against rules"""
        self.__validation_errors.clear()
        
        for column, rule in self.validation_rules.items():
            if not self._check_column_rule(df, column, rule):
                self.__validation_errors.append(f"Validation failed for {column}")
        
        self._validation_passed = len(self.__validation_errors) == 0
        return self._validation_passed
    
    def get_validation_errors(self) -> List[str]:
        """Public method to access private data - encapsulation"""
        return self.__validation_errors.copy()  # Return copy to maintain encapsulation
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Protected method for internal use"""
        # Apply business-specific transformations
        return df
    
    def _check_column_rule(self, df: DataFrame, column: str, rule: Dict) -> bool:
        """Protected method for rule checking"""
        rule_type = rule["type"]
        if rule_type == "not_null":
            return df.filter(df[column].isNull()).count() == 0
        elif rule_type == "range":
            return df.filter((df[column] < rule["min"]) | (df[column] > rule["max"])).count() == 0
        return True

# ===== POLYMORPHISM =====
class ShipmentDataValidator(BaseDataValidator):
    """Different validator demonstrating polymorphism"""
    
    def __init__(self):
        super().__init__()
        self.special_rules = {}
    
    def process_data(self, df: DataFrame) -> DataFrame:
        """Polymorphic implementation"""
        if self.validate_data(df):
            return self._apply_shipment_specific_rules(df)
        raise ValueError("Shipment data validation failed")
    
    def validate_data(self, df: DataFrame) -> bool:
        """Polymorphic validation implementation"""
        # Different validation logic for shipments
        return super().validate_data(df) and self._validate_shipment_specific_rules(df)
    
    def _validate_shipment_specific_rules(self, df: DataFrame) -> bool:
        """Shipment-specific validation"""
        return True
    
    def _apply_shipment_specific_rules(self, df: DataFrame) -> DataFrame:
        """Shipment-specific processing"""
        return df