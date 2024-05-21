from pyspark.ml.feature import VectorAssembler, StandardScaler

import src.db

from logger import Logger

SHOW_LOG = True

class Preprocess:
    logger = Logger(SHOW_LOG)
    log = logger.get_logger(__name__)
    def load_dataset(self, db: src.db.Database):
        dataset = db.read_table("OpenFoodFacts")

        vector_assembler = VectorAssembler(
            inputCols=dataset.columns,
            outputCol='features',
        )

        assembled_data = vector_assembler.transform(dataset)
        self.log.info("Assembled data")
        return assembled_data

    def scale_data(self, assembled_data):
        scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
        scaler_model = scaler.fit(assembled_data)
        data = scaler_model.transform(assembled_data)
        self.log.info("Scaled data")
        return data