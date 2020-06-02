package wikipedia

import java.time.LocalDateTime

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.scalamock.scalatest.MockFactory
import spark.{SparkAppTest, Storage}
import wikipedia.PageViewSchema.{Page, PageView}

class PageViewAppTest extends SparkAppTest with MockFactory {
  val pageViewsPath = "/wikipedia/pageviews-sample.txt"
  val blacklistPath = "/wikipedia/blacklist-domain-and-pages.txt"

  test("getHoursToProcess") {
    val bucketName = "dd-interview-data"
    val startDate = LocalDateTime.of(2020, 6, 1, 23, 0)
    val endDate = LocalDateTime.of(2020, 6, 2, 1, 0)

    val storageMock = mock[Storage]
    (storageMock.pathAlreadyExists _).expects(s"s3a://${bucketName}/wikipedia/pageviews/datetime=2020-06-01-23")
      .returning(false)
    (storageMock.pathAlreadyExists _).expects(s"s3a://${bucketName}/wikipedia/pageviews/datetime=2020-06-02-00")
      .returning(true)

    val expected = Seq((startDate, s"s3a://${bucketName}/wikipedia/pageviews/datetime=2020-06-01-23"))
    val result = PageViewApp.getHoursToProcess(storageMock, bucketName, startDate, endDate)
    assertResult(expected)(result)
  }

  test("getPageViewUrl") {
    val date = LocalDateTime.of(2020, 6, 1, 23, 0)
    val (baseUrl, fileName) = PageViewApp.getPageViewUrl(date)
    val expectedBaseUrl = "https://dumps.wikimedia.org/other/pageviews/2020/2020-06"
    val expectedFileName = "pageviews-20200602-000000.gz"
    assertResult((expectedBaseUrl, expectedFileName))((baseUrl, fileName))
  }

  test("filterPageViews") {
    val pageViewsURL = getClass.getResource(pageViewsPath).toString
    val blacklistURL = getClass.getResource(blacklistPath).toString
    import spark.implicits._
    val pageViews = storage.readCsv[PageView](pageViewsURL).as[PageView]
    val blacklist = storage.readCsv[Page](blacklistURL).as[Page]

    val filteredPageViews = PageViewApp.filterPageViews(pageViews, blacklist)

    val expectedFirstRow = Row("aa", "Special:ActiveUsers", 1)
    val expectedNumberOfRows = 3661
    val expectedPageViewCount = 5539
    assertResult(expectedFirstRow)(filteredPageViews.first())
    assertResult(expectedNumberOfRows)(filteredPageViews.count())
    assertResult(expectedPageViewCount)(filteredPageViews.agg(sum("count_views")).first.get(0))
  }

  test("getTopPagesPerDomain") {
    val pageViewsURL = getClass.getResource(pageViewsPath).toString
    val pageViews = storage.readCsv[PageView](pageViewsURL)

    val topPages = PageViewApp.getTopPagesPerDomain(spark, pageViews, PageViewApp.topN)

    val expectedFirstRow = Row("aa", "Main_Page", 19)
    val expectedNumberOfRows = 651
    val expectedPageViewCount = 2214
    assertResult(expectedFirstRow)(topPages.first())
    assertResult(expectedNumberOfRows)(topPages.count())
    assertResult(expectedPageViewCount)(topPages.agg(sum("count_views")).first.get(0))
  }

}
