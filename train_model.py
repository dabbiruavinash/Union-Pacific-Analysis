from models.base_model import BaseModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

class TrainDelayPredictor(BaseModel):
    def __init__(self, spark):
        super().__init__(spark)
        self.model = None
        self.pipeline = None
    
    def train(self, data):
        # Feature engineering
        assembler = VectorAssembler(
            inputCols=["current_speed", "weight", "car_count", "distance"],
            outputCol="features"
        )
        
        # Train model
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="delay_minutes",
            numTrees=100,
            maxDepth=10
        )
        
        self.pipeline = Pipeline(stages=[assembler, rf])
        self.model = self.pipeline.fit(data)
    
    def predict(self, data):
        return self.model.transform(data)
    
    def save_model(self, path):
        self.model.write().overwrite().save(path)
    
    def load_model(self, path):
        from pyspark.ml import PipelineModel
        self.model = PipelineModel.load(path)