from pyspark.sql.types import *
from enum import Enum

class FileFormats(Enum):
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    ORC = "orc"
    AVRO = "avro"

class DataPaths:
    RAW_DATA = "data/input/"
    PROCESSED_DATA = "data/processed/"
    OUTPUT_DATA = "data/output/"
    
class SCDType(Enum):
    TYPE1 = "scd1"
    TYPE2 = "scd2"
    TYPE3 = "scd3"