package spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.sum
import org.scalamock.scalatest.MockFactory
import wikipedia.PageViewSchema.PageView

class StorageTest extends SparkAppTest with MockFactory {
  val pageViewsPath = "/wikipedia/pageviews-sample.txt"

  test("readCsv") {
    val pageViewsURL = getClass.getResource(pageViewsPath).toString
    val pageViews = storage.readCsv[PageView](pageViewsURL)
    val expectedFirstRow = Row("aa", "Main_Page", 19)
    val expectedNumberOfRows = 3665
    val expectedPageViewCount = 5564
    assertResult(expectedFirstRow)(pageViews.first())
    assertResult(expectedNumberOfRows)(pageViews.count())
    assertResult(expectedPageViewCount)(pageViews.agg(sum("count_views")).first.get(0))
  }
}
