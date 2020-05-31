package spark

import org.apache.hadoop.fs.LocalFileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import scopt.OptionParser
import wikipedia.Config

trait SparkApp[T <: Config] {
  val LOGGER: Logger = Logger.getLogger(getClass.getName)
  def appName: String
  def configuration: T
  def configurationParser: OptionParser[T]

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config(buildSparkConfig())
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") // to pick up AWS_PROFILE

    parseAndRun(spark, args)
  }
  def run(spark: SparkSession, config: T, storage: Storage)

  private def buildSparkConfig(): SparkConf = {
    new SparkConf()
      .setAppName(appName)
      .set("fs.file.impl", classOf[LocalFileSystem].getName) // only to be run locally
      .set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") // to pick up AWS_PROFILE
      .setMaster("local[*]") // use config to set local?
  }
  private def parseAndRun(spark: SparkSession, args: Array[String]): Unit = {
    configurationParser.parse(args, configuration) match {
      case Some(config) => run(spark, config, new S3Storage(spark))
      case None => throw new IllegalArgumentException("Arguments provided are not valid")
    }
  }
}
