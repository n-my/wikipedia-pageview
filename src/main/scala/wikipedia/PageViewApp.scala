package wikipedia

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkFiles
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser
import spark.{SparkApp, Storage}
import util.DateUtil
import wikipedia.PageViewSchema.{Page, PageView}

object PageViewApp extends SparkApp[UsageConfig] {

  override def appName: String = "Wikipedia-PageViews"
  override def configuration: UsageConfig = UsageConfig()
  override def configurationParser: OptionParser[UsageConfig] = new UsageOptionParser()

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {
    import spark.implicits._

    LOGGER.info(s"The (included) start date is ${config.startDate.get}")
    LOGGER.info(s"The (excluded) end date is ${config.endDate.get}")
    LOGGER.info(s"The output bucket is ${config.bucketName}")

    // Get hours to process
    val hours = getHoursToProcess(storage, config.bucketName, config.startDate.get, config.endDate.get)
    if (hours.isEmpty) return

    // Load black list as Dataset
    val blacklistBaseUrl = "https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia"
    val blacklistFileName = "blacklist_domains_and_pages"
    spark.sparkContext.addFile(s"${blacklistBaseUrl}/${blacklistFileName}")
    val blacklistedPages: Dataset[Page] = storage
      .readCsv[Page](s"file:///${SparkFiles.get(blacklistFileName)}")
      .as[Page]

    hours
      .foreach{ case (hour, outputPath) =>
        LOGGER.info(s"Processing the hour ${hour}")

        // Load page view counts as Dataset
        val (fileBaseUrl, fileName) = getPageViewUrl(hour)
        spark.sparkContext.addFile(s"${fileBaseUrl}/${fileName}")
        val pageViews: Dataset[PageView] = storage
          .readCsv[PageView](s"file:///${SparkFiles.get(fileName)}")
          .as[PageView]

        // Filter out blacklisted pages
        val filteredPageViews = pageViews.join(blacklistedPages,
          pageViews("domain_code") === blacklistedPages("domain_code") &&
            pageViews("page_title") === blacklistedPages("page_title"),
          "left_anti")

        // Get top 25 pages per domain
        val overDomain = Window.partitionBy('domain_code).orderBy('domain_code.asc, 'count_views.desc)
        val topPageViews = filteredPageViews
          .withColumn("rank", row_number.over(overDomain))
          .where('rank <= 25)
          .drop('rank)
          .coalesce(1)

        // Write hourly output
        storage.writeCsv(topPageViews, outputPath)
      }
  }

  def getHoursToProcess(storage: Storage, bucketName: String, startDate: LocalDateTime, endDate: LocalDateTime): Seq[(LocalDateTime, String)] = {
    val outputDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
    DateUtil.getLocalDateTimeRange(startDate, endDate)
      .map(date => {
        val outputPath = s"s3a://${bucketName}/wikipedia/pageviews/datetime=${date.format(outputDateFormatter)}"
        (date, outputPath)
      })
      .filter{ case (_, outputPath) => !storage.pathAlreadyExists(outputPath)}
  }

  def getPageViewUrl(date: LocalDateTime): (String, String) = {
    val datePart1 = date.format(DateTimeFormatter.ofPattern("yyyy/yyyy-MM"))
    val baseUrl = s"https://dumps.wikimedia.org/other/pageviews/${datePart1}"
    val datePart2 = date.format(DateTimeFormatter.ofPattern("yyyyMMdd-HH0000"))
    val fileName = s"pageviews-${datePart2}.gz"
    (baseUrl, fileName)
  }
}
