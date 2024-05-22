import os
import path
import configparser
import pandas as pd

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession

from db import Database
from datamart import DataMart

from logger import Logger

SHOW_LOG = True


class KMeansModel:
    def __init__(self, datamart):
        self.datamart = datamart
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)
        self.log.info('KmeansEvaluator initialized.')

    def clustering(self, final_data):
        silhouette_score = []
        evaluator = ClusteringEvaluator(predictionCol='prediction',
                                        featuresCol='scaled_features',
                                        metricName='silhouette',
                                        distanceMeasure='squaredEuclidean')

        kmeans = KMeans(featuresCol='scaled_features', k=2)
        model = kmeans.fit(final_data)
        predictions = model.transform(final_data)
        score = evaluator.evaluate(predictions)
        silhouette_score.append(score)
        print('Silhouette Score for k =', 2, 'is', score)

        self.datamart.write_predictions(predictions.select("prediction"))
        self.log.info('Kmeans trained successfully.')


def main():
    main_path = path.Path(__file__).absolute()
    main_path = main_path.parent.parent
    config = configparser.ConfigParser()
    config.read(os.path.join(main_path, 'config.ini'))

    spark = SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .master(config['spark']['deploy_mode']) \
        .config("spark.driver.memory", config['spark']['driver_memory']) \
        .config("spark.executor.memory", config['spark']['executor_memory']) \
        .config("spark.driver.extraClassPath", config['spark']['mysql_connector']) \
        .config("spark.jars", f"{config['spark']['datamart']}, {config['spark']['config']}") \
        .getOrCreate()

    db = Database(spark)
    path_to_data = os.path.join(main_path, config['data']['openfood'])
    df = pd.read_csv(path_to_data)
    df.columns = df.columns.str.replace('-', '_')
    df = df.drop(columns=['fruits_vegetables_nuts_estimate_from_ingredients_100g'], errors='ignore')
    # Определяем названия столбцов и их типы данных
    columns = {
        "energy_kcal_100g": "FLOAT",
        "energy_100g": "FLOAT",
        "fat_100g": "FLOAT",
        "saturated_fat_100g": "FLOAT",
        "carbohydrates_100g": "FLOAT",
        "sugars_100g": "FLOAT",
        "proteins_100g": "FLOAT",
        "salt_100g": "FLOAT",
        "sodium_100g": "FLOAT"
    }

    # Создаем таблицу с указанными столбцами
    db.create_table(config['mysql']['tablename'], columns)

    db.insert_data(config['mysql']['tablename'], df)

    datamart = DataMart(spark)

    assembled_data = datamart.read_dataset("OpenFoodFacts")
    kmeans = KMeansModel(datamart)
    kmeans.clustering(assembled_data)
    assembled_data.collect()
    spark.stop()


if __name__ == '__main__':
    main()
