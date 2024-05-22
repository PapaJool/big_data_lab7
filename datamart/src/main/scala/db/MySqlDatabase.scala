package db

import org.apache.spark.sql.{DataFrame, SparkSession}
import logging.MyLoggerFactory
import config.ConfigLoader._

class MySqlDatabase(spark: SparkSession) {
  private val logger = MyLoggerFactory.getLogger(getClass)

  private val JDBC_URL = s"jdbc:mysql://${mysql.host}:${mysql.port}/${mysql.database}"

  def readTable(tablename: String): DataFrame = {
    logger.info(s"Reading table $tablename")
    val df = spark.read
      .format("jdbc")
      .option("url", JDBC_URL)
      .option("user", mysql.username)
      .option("password", mysql.password)
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
      .option("user", mysql.username)
      .option("password", mysql.password)
      .option("dbtable", tablename)
      .mode("overwrite")
      .save()
    logger.info(s"Inserted ${df.count()} records into table $tablename")
  }
}
