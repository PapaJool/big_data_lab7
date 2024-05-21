from pyspark.sql import DataFrame, SparkSession, SQLContext

from logger import Logger

SHOW_LOG = True
class DataMart:
    def __init__(self, spark: SparkSession):
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)
        self.spark_context = spark.sparkContext
        self.sql_context = SQLContext(self.spark_context, spark)
        self.jwm_datamart = self.spark_context._jvm.DataMart
        self.log.info("Data Mart initialization")

    def read_dataset(self) -> DataFrame:
        jvm_data = self.jwm_datamart.readPreprocessedOpenFoodFactsDataset()
        self.log.info("Data Mart read dataset")
        return DataFrame(jvm_data, self.sql_context)

    def write_predictions(self, df: DataFrame):
        self.log.info("Data Mart write predictions")
        self.jwm_datamart.writePredictions(df._jdf)