import org.apache.spark.sql.{DataFrame, SparkSession}
import preprocess.Preprocessor
import db.MySqlDatabase
import logging.MyLoggerFactory

object DataMart {
  private val logger = MyLoggerFactory.getLogger(getClass)
  val session: SparkSession = SparkSession.builder
    .appName("kMeans")
    .master("local")
    .config("spark.driver.cores", 1)
    .config("spark.executor.cores", 1)
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1g")
    .config("spark.jars", "../mysql-connector-j-8.4.0.jar")
    .config("spark.driver.extraClassPath", "../mysql-connector-j-8.4.0.jar")
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/datamart/src/main/resources/log4j2.xml")
    .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:/datamart/src/main/resources/log4j2.xml")
    .getOrCreate()


  private val db = new MySqlDatabase(session)

  def readPreprocessedOpenFoodFactsDataset(): DataFrame = {
    logger.info("Reading and preprocessing OpenFoodFacts dataset")
    val data = db.readTable("OpenFoodFacts")
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
