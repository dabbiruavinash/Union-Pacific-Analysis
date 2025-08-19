from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

# ===== ADAPTER PATTERN =====
class LegacyDataProcessor:
    """Legacy system that needs to be adapted"""
    
    def process_legacy_data(self, data_input: dict) -> dict:
        """Legacy method with different interface"""
        # Simulate legacy processing
        return {"processed": True, "legacy_result": "data_processed"}
    
    def validate_legacy_format(self, data_input: dict) -> bool:
        return "required_field" in data_input

class ModernDataProcessor(ABC):
    """Modern interface that we want to use"""
    
    @abstractmethod
    def process_dataframe(self, df: DataFrame) -> DataFrame:
        pass
    
    @abstractmethod
    def validate_dataframe(self, df: DataFrame) -> bool:
        pass

class LegacyProcessorAdapter(ModernDataProcessor):
    """Adapter that makes legacy processor work with modern interface"""
    
    def __init__(self, legacy_processor: LegacyDataProcessor):
        self.legacy_processor = legacy_processor
    
    def process_dataframe(self, df: DataFrame) -> DataFrame:
        # Convert DataFrame to legacy format
        legacy_data = self._convert_dataframe_to_legacy(df)
        
        # Process using legacy system
        result = self.legacy_processor.process_legacy_data(legacy_data)
        
        # Convert back to DataFrame
        return self._convert_legacy_to_dataframe(result)
    
    def validate_dataframe(self, df: DataFrame) -> bool:
        legacy_data = self._convert_dataframe_to_legacy(df)
        return self.legacy_processor.validate_legacy_format(legacy_data)
    
    def _convert_dataframe_to_legacy(self, df: DataFrame) -> dict:
        # Convert DataFrame to legacy dictionary format
        collected_data = df.collect()
        return {
            "rows": [row.asDict() for row in collected_data],
            "schema": [str(field) for field in df.schema.fields]
        }
    
    def _convert_legacy_to_dataframe(self, legacy_result: dict) -> DataFrame:
        # Convert legacy result back to DataFrame
        # This would need actual implementation based on legacy format
        return None