package wikipedia

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkFiles
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scopt.OptionParser
import spark.{SparkApp, Storage}
import util.DateUtil
import wikipedia.PageViewSchema.{Page, PageView}

/**
 * A Spark application that
 * - reads Wikipedia page views CSV file(s) for given date and hours
 * - filter out blacklisted pages
 * - calculates the top N viewed pages per domain
 * - writes the result as CSV file(s) to S3
 */
object PageViewApp extends SparkApp[UsageConfig] {

  override def appName: String = "Wikipedia-PageViews"
  override def configuration: UsageConfig = UsageConfig()
  override def configurationParser: OptionParser[UsageConfig] = new UsageOptionParser()
  val topN = 25
  val OutputPartitionNumber = 1

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {
    logger.info(s"The (included) start date is ${config.startDate.get}")
    logger.info(s"The (excluded) end date is ${config.endDate.get}")
    logger.info(s"The output bucket is ${config.bucketName}")

    try {
      // Get hours to process
      val hours = getHoursToProcess(storage, config.bucketName, config.startDate.get, config.endDate.get)
      if (hours.isEmpty) return

      // Load blacklist as Dataset
      val blacklistedPages: Dataset[Page] = loadBlacklistedPages(spark, storage)

      // Process
      hours.foreach{ case (hour, outputPath) =>
          logger.info(s"Processing the hour ${hour}")
          val pageViews: Dataset[PageView] = loadPageViews(spark, storage, hour)
          val filteredPageViews = filterPageViews(pageViews, blacklistedPages)
          val topPageViews = getTopPagesPerDomain(spark, filteredPageViews, topN)
          storage.writeCsv(topPageViews, OutputPartitionNumber, outputPath)
      }
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }

  /**
   * Returns the range of hours to process between `startDate` and `endDate`,
   * with their respective output paths.
   *
   * For a given hour, if the S3 output path already exists, then it is not returned.
   *
   * @param storage storage used to check if the output path already exists
   * @param bucketName S3 bucket name
   * @param startDate the start date (included) in UTC
   * @param endDate the end date (excluded) in UTC
   * @return the collection of (hour, outputPath) to process
   */
  def getHoursToProcess(storage: Storage, bucketName: String, startDate: LocalDateTime, endDate: LocalDateTime): Seq[(LocalDateTime, String)] = {
    val outputDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
    DateUtil.getHoursRange(startDate, endDate)
      // Get the output paths
      .map(date => {
        val outputPath = s"s3a://${bucketName}/wikipedia/pageviews/datetime=${date.format(outputDateFormatter)}"
        (date, outputPath)
      })
      // Filter out already processed hours
      .filter{ case (_, outputPath) => !storage.pathAlreadyExists(outputPath)}
  }

  /**
   * Downloads the blacklist CVS file from HTTP to every Spark node, and
   * loads this file to a Dataset of [[Page]]
   *
   * @param spark the job [[SparkSession]]
   * @param storage the job [[Storage]]
   * @return the Dataset of [[Page]]
   */
  def loadBlacklistedPages(spark: SparkSession, storage: Storage): Dataset[Page] = {
    import spark.implicits._
    val blacklistBaseUrl = "https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia"
    val blacklistFileName = "blacklist_domains_and_pages"
    spark.sparkContext.addFile(s"${blacklistBaseUrl}/${blacklistFileName}")
    storage
      .readCsv[Page](s"file:///${SparkFiles.get(blacklistFileName)}")
      .as[Page]
  }

  /**
   * Returns the base URL and the filename of the pageviews file
   * for a given hour.
   *
   * Note: the time used in the wikipedia filename refers to the end
   * of the aggregation period, not the beginning.
   *
   * @param date the given hour, in UTC
   * @return e.g. `("https://dumps.wikimedia.org/other/pageviews/2020/2020-06", "pageviews-20200601-150000.gz")`
   */
  def getPageViewUrl(date: LocalDateTime): (String, String) = {
    val fileDate = date.plusHours(1)
    val datePart1 = fileDate.format(DateTimeFormatter.ofPattern("yyyy/yyyy-MM"))
    val baseUrl = s"https://dumps.wikimedia.org/other/pageviews/${datePart1}"
    val datePart2 = fileDate.format(DateTimeFormatter.ofPattern("yyyyMMdd-HH0000"))
    val fileName = s"pageviews-${datePart2}.gz"
    (baseUrl, fileName)
  }

  /**
   * Downloads the pageviews CVS file from HTTP to every Spark node, and
   * loads this file to a Dataset of [[PageView]]
   *
   * @param spark the job [[SparkSession]]
   * @param storage the job [[Storage]]
   * @return the Dataset of [[PageView]]
   */
  def loadPageViews(spark: SparkSession, storage: Storage, hour: LocalDateTime): Dataset[PageView] = {
    import spark.implicits._
    val (fileBaseUrl, fileName) = getPageViewUrl(hour)
    spark.sparkContext.addFile(s"${fileBaseUrl}/${fileName}")
    storage
      .readCsv[PageView](s"file:///${SparkFiles.get(fileName)}")
      .as[PageView]
  }

  /**
   * Remove any [[org.apache.spark.sql.Row]] from the `pageViews` dataset
   * if there is a [[org.apache.spark.sql.Row]] in the `blacklist` dataset
   * that has the same `domain_code` and same `page_title`
   *
   * @return the filtered dataset
   */
  def filterPageViews(pageViews: Dataset[PageView], blacklist: Dataset[Page]): DataFrame = {
    pageViews
      .join(
        blacklist,
        pageViews("domain_code") === blacklist("domain_code") &&
          pageViews("page_title") === blacklist("page_title"),
        LeftAnti.toString
      )
  }

  /**
   * Computes a Dataframe to get:
   * - the `topN` pages per `domain_code` and in terms of `count_views`
   * - sorted per `domain_code` and `count_views`
   * from the given `pageViews` [[DataFrame]]
   *
   * @param spark the job [[SparkSession]]
   * @param pageViews the given `pageViews` [[DataFrame]]
   * @param topN number of pages to keep per `domain_code`
   * @return the computed [[DataFrame]]
   */
  def getTopPagesPerDomain(spark: SparkSession, pageViews: DataFrame, topN: Int): DataFrame = {
    import spark.implicits._
    val overDomain = Window
      .partitionBy("domain_code")
      .orderBy($"count_views".desc)
    pageViews
      .withColumn("rank", functions.row_number.over(overDomain))
      .where($"rank" <= topN)
      .drop($"rank")
      .orderBy($"domain_code".asc, $"count_views".desc)
  }
}
