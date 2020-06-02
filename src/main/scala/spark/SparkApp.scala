package spark

import org.apache.hadoop.fs.LocalFileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import wikipedia.Config

import scala.util.Properties

/**
 * Trait to deal with the [[SparkSession]], and the configuration of a Spark job.
 * @tparam T
 */
trait SparkApp[T <: Config] {
  val logger: Logger = Logger.getLogger(getClass.getName)
  def appName: String
  def configuration: T
  def configurationParser: OptionParser[T]

  /**
   * Parses the arguments and runs the Spark job
   */
  def main(args: Array[String]): Unit = {
    // Parse args and run Spark job
    configurationParser.parse(args, configuration) match {
      case Some(config) => run(spark, config, new Storage(spark))
      case None => throw new IllegalArgumentException("Arguments provided are not valid")
    }
  }

  /**
   * Runs the Spark job
   * @param spark the job [[SparkSession]]
   * @param config the job configuration
   * @param storage the storage interface
   */
  def run(spark: SparkSession, config: T, storage: Storage)

  /**
   * Returns the current [[SparkSession]] or builds one if none.
   *
   * We set `fs.s3a.aws.credentials.provider` in order to enable all credential providers
   * the AWS cli supports, such as the `AWS_PROFILE` env var.
   */
  def spark: SparkSession = {
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") // to pick up AWS_PROFILE
    spark
  }

  /**
   * Builds and returns the [[SparkConf]].
   *
   * Some settings are only set for local standalone runs, based on the `SPARK_LOCAL_RUN` env var
   */
  def sparkConf: SparkConf = {
    val conf = new SparkConf()
      .setAppName(appName)
      .set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") // to pick up AWS_PROFILE
    if (Properties.envOrElse("SPARK_LOCAL_RUN", "false").toBoolean) {
      conf.setMaster("local[*]")
        .set("fs.file.impl", classOf[LocalFileSystem].getName)
    }
    conf
  }
}
