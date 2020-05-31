package spark

import java.net.URI

import reflect.runtime.universe.TypeTag
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait Storage {
  def pathAlreadyExists(location: String): Boolean
  def readCsv[T](location: String)(implicit tag:TypeTag[T]): DataFrame
  def writeCsv[T](dataset: Dataset[T], location: String): Unit
}

class S3Storage(spark: SparkSession) extends Storage {
  val logger: Logger = Logger.getLogger(getClass.getName)

  def pathAlreadyExists(location: String): Boolean = {
    val fs = FileSystem.get(URI.create(location), spark.sparkContext.hadoopConfiguration)
    val alreadyProcessed = fs.exists(new Path(location))
    if (alreadyProcessed) {
      logger.info(s"${location} has already been processed.")
    }
    alreadyProcessed
  }
  def readCsv[T](location: String)(implicit tag:TypeTag[T]): DataFrame = {
    spark
      .read
      .option("header", "false")
      .option("sep", " ")
      .schema(ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType])
      .csv(location)
  }
  def writeCsv[T](dataset: Dataset[T], location: String): Unit = {
    dataset
      .write
      .option("sep", " ")
      .option("codec", "gzip")
      .csv(location)
  }
}
