package preprocess

import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame
import logging.MyLoggerFactory

object Preprocessor {
  private val logger = MyLoggerFactory.getLogger(getClass)

  def assembleVector(df: DataFrame): DataFrame = {
    val columnToRemove = "sodium_100g"
    logger.info(s"Removing column $columnToRemove from DataFrame")
    val filteredDf = df.drop(columnToRemove)

    logger.info("Assembling vector from DataFrame columns")
    val outputCol = "features"
    val inputCols = filteredDf.columns

    val vectorAssembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol(outputCol)
    val assembledDf = vectorAssembler.transform(filteredDf)
    logger.debug("Assembled vector schema: " + assembledDf.schema)
    assembledDf
  }

  def scaleAssembledDataset(df: DataFrame): DataFrame = {
    logger.info("Scaling assembled dataset")
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
    val scalerModel = scaler.fit(df)
    val scaledDf = scalerModel.transform(df)
    logger.debug("Scaled dataset schema: " + scaledDf.schema)
    scaledDf
  }
}
