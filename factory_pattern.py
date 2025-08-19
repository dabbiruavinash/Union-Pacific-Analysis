from typing import Dict, Type
from core.oop_foundation import DataProcessor, TrainDataValidator, ShipmentDataValidator

class DataProcessorFactory:
    """Factory class demonstrating creational patterns"""
    
    # Class variable - shared across all instances
    _processor_registry: Dict[str, Type[DataProcessor]] = {}
    
    @classmethod
    def register_processor(cls, data_type: str, processor_class: Type[DataProcessor]):
        """Register processor class for specific data type"""
        cls._processor_registry[data_type] = processor_class
    
    @classmethod
    def create_processor(cls, data_type: str, *args, **kwargs) -> DataProcessor:
        """Factory method to create appropriate processor"""
        if data_type not in cls._processor_registry:
            raise ValueError(f"No processor registered for data type: {data_type}")
        
        processor_class = cls._processor_registry[data_type]
        return processor_class(*args, **kwargs)
    
    @classmethod
    def get_available_processors(cls) -> List[str]:
        """Get list of available processor types"""
        return list(cls._processor_registry.keys())

# Register processors
DataProcessorFactory.register_processor("train", TrainDataValidator)
DataProcessorFactory.register_processor("shipment", ShipmentDataValidator)