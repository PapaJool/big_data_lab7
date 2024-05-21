package db

import org.apache.spark.sql.{DataFrame, SparkSession}
import logging.MyLoggerFactory

class MySqlDatabase(spark: SparkSession) {
  private val logger = MyLoggerFactory.getLogger(getClass)
  private val JDBC_URL = s"jdbc:mysql://127.0.0.1:55001/lab6_bd"

  def readTable(tablename: String): DataFrame = {
    logger.info(s"Reading table $tablename")
    val df = spark.read
      .format("jdbc")
      .option("url", JDBC_URL)
      .option("user", "root")
      .option("password", "0000")
      .option("dbtable", tablename)
      .option("inferSchema", "true")
      .load()
    logger.info(s"Read ${df.count()} records from table $tablename")
    df
  }

  def insertDf(df: DataFrame, tablename: String): Unit = {
    logger.info(s"Inserting DataFrame into table $tablename")
    df.write
      .format("jdbc")
      .option("url", JDBC_URL)
      .option("user", "root")
      .option("password", "0000")
      .option("dbtable", tablename)
      .mode("append")
      .save()
    logger.info(s"Inserted ${df.count()} records into table $tablename")
  }
}
