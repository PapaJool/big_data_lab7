import os
import mysql.connector
import pandas as pd
from typing import Dict
import numpy as np
from logger import Logger

SHOW_LOG = True

class Database():
    def __init__(self, spark, host="0.0.0.0", port=55001, database="lab6_bd"):
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)
        self.username = "root"
        self.password = "0000"
        self.spark = spark
        self.client = mysql.connector.connect(
                                    user=self.username,
                                    password=self.password,
                                    database=database,
                                    host=host,
                                    port=port)
        self.jdbcUrl = f"jdbc:mysql://{host}:{port}/{database}"
        self.log.info("Initializing database")

    def read_table(self, tablename: str):
        self.log.info(f"Reading table {tablename}")
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbcUrl) \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("dbtable", tablename) \
            .option("inferSchema", "true") \
            .load()


    def insert_df(self, df, tablename):
        self.log.info(f"Inserting dataframe {tablename}")
        df.write \
            .format("jdbc") \
            .option("url", self.jdbcUrl) \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("dbtable", tablename) \
            .mode("overwrite") \
            .save()

    def execute_query(self, query):
        try:
            with self.client.cursor() as cursor:
                cursor.execute(query)
            self.client.commit()
            self.log.info("Query executed successfully!")
        except Exception as e:
            self.log.warn(f"Error executing query: {e}")

    def create_table(self, table_name: str, columns: Dict):
        self.log.info(f"Creating table {table_name}")
        cols = ", ".join([f"`{k}` {v}" for k, v in columns.items()])
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} 
            (
                {cols}
            )
            ENGINE = InnoDB;
        """
        self.execute_query(query)

    def insert_data(self, table_name: str, df):
        columns = ", ".join([f"`{col}`" for col in df.columns])
        placeholders = ", ".join(["%s" for _ in range(len(df.columns))])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            with self.client.cursor() as cursor:
                # Преобразуем DataFrame в список кортежей
                data = [tuple(row) for row in df.to_numpy()]
                # Вставляем данные в таблицу
                cursor.executemany(query, data)
            self.client.commit()
            self.log.info(f"Data inserted successfully! {table_name}")
        except Exception as e:
            self.log.warn(f"Error inserting data: {e}")

