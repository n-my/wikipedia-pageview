package spark

import java.io.IOException
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * A class to interface with the external storage systems
 * for sources and sinks of a Spark job
 * @param spark the [[SparkSession]] of the current job
 */
class Storage(spark: SparkSession) {
  val logger: Logger = Logger.getLogger(getClass.getName)

  /**
   *
   * Reads a CSV file at a given location and returns a `DataFrame`
   * Uses the schema of the type T
   *
   * By default, no header, ' ' as the delimiter and invalid tokens are
   * returned as [[org.apache.spark.sql.Row]] with null values. Use `options` to override.
   *
   * @param location the location of the CSV file
   * @param options (optional) options to override the defaults of the [[org.apache.spark.sql.DataFrameReader]]
   * @tparam T Scala case class used as the schema
   * @return a [[DataFrame]]
   */
  def readCsv[T](location: String, options: Option[Map[String, String]] = None)(implicit tag: TypeTag[T]): DataFrame = {
    val defaultOpt = Map(
      "header" -> "false",
      "sep"-> " ",
      "mode"-> "PERMISSIVE"
    )
    val opt = options.getOrElse(defaultOpt)
    spark
      .read
      .options(opt)
      .schema(ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType])
      .csv(location)
  }

  /**
   *
   * Saves the content of a [[DataFrame]] in CSV format to the specified location.
   *
   * By default, ' ' as the delimiter and compressed in gzip. Use `options` to override.
   *
   * @param dataframe the dataframe to save
   * @param numPartitions the number of output files
   * @param location the output location
   * @param options (optional) options to override the defaults of the [[org.apache.spark.sql.DataFrameWriter]]
   */
  def writeCsv(dataframe: DataFrame, numPartitions: Int, location: String,
                  options: Option[Map[String, String]] = None): Unit = {
    val defaultOpt = Map(
      "sep"-> " ",
      "codec" -> "gzip"
    )
    val opt = options.getOrElse(defaultOpt)
    dataframe
      .coalesce(numPartitions)
      .write
      .options(opt)
      .csv(location)
    logger.info(s"Output saved to ${location}/")
  }

  /**
   * Checks if the given location exists
   * @param location the given location, e.g. `s3a://mybucket/output/datetime=2020-06-01/`
   * @throws java.io.IOException from [[FileSystem]]
   * @return `True` if exists, `False` otherwise
   */
  @throws(classOf[IOException])
  def pathAlreadyExists(location: String): Boolean = {
    try {
      val fs = FileSystem.get(URI.create(location), spark.sparkContext.hadoopConfiguration)
      val alreadyProcessed = fs.exists(new Path(location))
      if (alreadyProcessed) {
        logger.info(s"${location} already exists.")
      }
      alreadyProcessed
    } catch {
      case e: IOException => {
        logger.error(s"Error while checking if the path ${location} existed.")
        throw e
      }
    }
  }
}
