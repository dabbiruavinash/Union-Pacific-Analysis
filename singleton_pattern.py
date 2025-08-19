from threading import Lock
from pyspark.sql import SparkSession

class SingletonMeta(type):
    """Metaclass for implementing Singleton pattern"""
    
    _instances = {}
    _lock: Lock = Lock()
    
    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]

class SparkSessionManager(metaclass=SingletonMeta):
    """Singleton Spark session manager"""
    
    def __init__(self, app_name: str = "UnionPacificAnalysis"):
        self.spark = self._create_spark_session(app_name)
    
    def _create_spark_session(self, app_name: str) -> SparkSession:
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def get_spark_session(self) -> SparkSession:
        return self.spark
    
    def stop_spark_session(self):
        if self.spark:
            self.spark.stop()
            # Remove instance from registry
            if self.__class__ in self.__class__._instances:
                del self.__class__._instances[self.__class__]

class ConfigurationManager(metaclass=SingletonMeta):
    """Singleton configuration manager"""
    
    def __init__(self, config_path: str = "config/app_config.json"):
        self.config_path = config_path
        self.config = self._load_configuration()
    
    def _load_configuration(self) -> dict:
        # Load configuration from file
        return {"default": "config"}  # Simplified
    
    def get_config(self, key: str, default=None):
        return self.config.get(key, default)