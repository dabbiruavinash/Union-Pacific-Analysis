from pyspark import AccumulatorParam

class CustomAccumulator:
    class DictAccumulatorParam(AccumulatorParam):
        def zero(self, initialValue):
            return initialValue.copy()
        
        def addInPlace(self, v1, v2):
            for key in v2:
                if key in v1:
                    v1[key] += v2[key]
                else:
                    v1[key] = v2[key]
            return v1
    
    def __init__(self, sc, initial_value=None):
        self.sc = sc
        initial = initial_value or {}
        self.accumulator = sc.accumulator(initial, self.DictAccumulatorParam())
    
    def add(self, value):
        self.accumulator.add(value)
    
    def value(self):
        return self.accumulator.value