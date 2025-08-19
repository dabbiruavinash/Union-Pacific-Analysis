from pyspark import AccumulatorParam
from typing import Dict, List, Any

class AdvancedAccumulators:
    class ListAccumulatorParam(AccumulatorParam):
        def zero(self, initialValue):
            return initialValue.copy()
        
        def addInPlace(self, v1, v2):
            v1.extend(v2)
            return v1
    
    class SetAccumulatorParam(AccumulatorParam):
        def zero(self, initialValue):
            return initialValue.copy()
        
        def addInPlace(self, v1, v2):
            v1.update(v2)
            return v1
    
    def __init__(self, sc):
        self.sc = sc
        self.accumulators: Dict[str, Any] = {}
    
    def create_accumulator(self, name: str, acc_type: str, initial_value=None):
        """Create different types of accumulators"""
        if acc_type == "dict":
            param = self.DictAccumulatorParam()
            initial = initial_value or {}
        elif acc_type == "list":
            param = self.ListAccumulatorParam()
            initial = initial_value or []
        elif acc_type == "set":
            param = self.SetAccumulatorParam()
            initial = initial_value or set()
        else:
            raise ValueError(f"Unsupported accumulator type: {acc_type}")
        
        accumulator = self.sc.accumulator(initial, param)
        self.accumulators[name] = accumulator
        return accumulator
    
    def get_accumulator_value(self, name: str):
        """Get accumulator value"""
        return self.accumulators[name].value if name in self.accumulators else None