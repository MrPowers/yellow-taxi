package mrpowers.yellow.taxi

import org.apache.spark.sql.SaveMode

object TaxiLakeCreator extends SparkSessionWrapper {

  def createParquetLake(): Unit = {
    val path = new java.io.File("./src/main/resources/taxi_data/").getCanonicalPath
    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)

    val outputPath = new java.io.File("./tmp/parquet_lake/").getCanonicalPath
    df
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }

  def createDeltaLake(): Unit = {
    val path = new java.io.File("./src/main/resources/taxi_data/").getCanonicalPath
    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)

    val outputPath = new java.io.File("./tmp/delta_lake/").getCanonicalPath
    df
      .repartition(1)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(outputPath)
  }

  def createIncrementalDeltaLake(): Unit = {
    val outputPath = new java.io.File("./tmp/incremental_delta_lake/").getCanonicalPath

    val p1 = new java.io.File("./src/main/resources/taxi_data/taxi1.csv").getCanonicalPath
    val df1 = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(p1)
    df1
      .repartition(1)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(outputPath)

    val p2 = new java.io.File("./src/main/resources/taxi_data/taxi2.csv").getCanonicalPath
    val df2 = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(p2)
    df2
      .repartition(1)
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(outputPath)

//    val p3 = new java.io.File("./src/main/resources/taxi_data/taxi3.csv").getCanonicalPath
//    val df3 = spark
//      .read
//      .option("header", "true")
//      .option("charset", "UTF8")
//      .csv(p3)
//    df3
//      .repartition(1)
//      .write
//      .format("delta")
//      .mode(SaveMode.Append)
//      .save(outputPath)
  }

}
