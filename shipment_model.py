from models.base_model import BaseModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

class ShipmentDelayPredictor(BaseModel):
    def __init__(self, spark):
        super().__init__(spark)
        self.model = None
        self.pipeline = None
    
    def train(self, data):
        # Feature engineering
        indexer = StringIndexer(inputCol="commodity_type", outputCol="commodity_index")
        encoder = OneHotEncoder(inputCol="commodity_index", outputCol="commodity_encoded")
        
        assembler = VectorAssembler(
            inputCols=["weight", "volume", "commodity_encoded", "priority"],
            outputCol="features"
        )
        
        # Train classifier
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="is_delayed",
            numTrees=50,
            maxDepth=8
        )
        
        self.pipeline = Pipeline(stages=[indexer, encoder, assembler, rf])
        self.model = self.pipeline.fit(data)
    
    def predict(self, data):
        return self.model.transform(data)
    
    def evaluate(self, data):
        predictions = self.predict(data)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="is_delayed",
            predictionCol="prediction",
            metricName="accuracy"
        )
        return evaluator.evaluate(predictions)