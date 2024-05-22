import org.apache.spark.sql.{DataFrame, SparkSession}
import preprocess.Preprocessor
import db.MySqlDatabase
import logging.MyLoggerFactory
import config.ConfigLoader._

object DataMart {
  private val logger = MyLoggerFactory.getLogger(getClass)
  val session: SparkSession = SparkSession.builder
    .appName(spark.appName)
    .master(spark.deployMode)
    .config("spark.driver.cores", spark.driverCore)
    .config("spark.executor.cores", spark.executorCore)
    .config("spark.driver.memory", spark.driverMemory)
    .config("spark.executor.memory", spark.executorMemory)
    .config("spark.jars", s"${spark.mysqlConnector}:${spark.configS}")
    .config("spark.driver.extraClassPath", s"${spark.mysqlConnector}:${spark.configS}")
    .config("spark.driver.extraJavaOptions", spark.logger)
    .config("spark.executor.extraJavaOptions", spark.logger)
    .getOrCreate()

  private val db = new MySqlDatabase(session)

  def readPreprocessed(tablename: String): DataFrame = {
    logger.info("Reading and preprocessing OpenFoodFacts dataset")
    val data = db.readTable(tablename)
    val transforms: Seq[DataFrame => DataFrame] = Seq(
      Preprocessor.assembleVector,
      Preprocessor.scaleAssembledDataset
    )
    val transformed = transforms.foldLeft(data) { (df, f) => f(df) }
    logger.debug("Preprocessed dataset schema: " + transformed.schema)
    transformed
  }

  def writePredictions(df: DataFrame): Unit = {
    logger.info("Writing predictions to database")
    db.insertDf(df, "Predictions")
    logger.info("Predictions written to Predictions table")
  }
}
