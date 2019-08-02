package mrpowers.yellow.taxi

import org.apache.spark.sql.SaveMode

object LakeCreator extends SparkSessionWrapper {

  def createParquetLake(): Unit = {
    val path = new java.io.File("./src/main/resources/").getCanonicalPath
    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)
    val outputPath = new java.io.File("./tmp/parquet_lake/").getCanonicalPath
    df.repartition(1).write.mode(SaveMode.Overwrite).parquet(outputPath)
  }

}
