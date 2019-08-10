package mrpowers.yellow.taxi

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object IncrementalDogUpdater extends SparkSessionWrapper {

  def update(): Unit = {
    val csvPath = new java.io.File("./tmp/dog_data_csv/").getCanonicalPath

    val schema = StructType(
      List(
        StructField("first_name", StringType, true),
        StructField("breed", StringType, true)
      )
    )

    val df = spark.readStream
      .schema(schema)
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(csvPath)

    val checkpointPath = new java.io.File("./tmp/dog_data_checkpoint/").getCanonicalPath
    val parquetPath = new java.io.File("./tmp/dog_data_parquet/").getCanonicalPath

    df
      .writeStream
      .trigger(Trigger.Once)
      .format("parquet")
      .option("checkpointLocation", checkpointPath)
      .start(parquetPath)

    spark.read.parquet(parquetPath).show()
  }

  def showDogDataParquet(): Unit = {
    val parquetPath = new java.io.File("./tmp/dog_data_parquet/").getCanonicalPath
    spark.read.parquet(parquetPath).show()
  }

}
