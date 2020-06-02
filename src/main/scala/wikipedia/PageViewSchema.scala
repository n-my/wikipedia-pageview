package wikipedia

/**
 * Object that holds schemas useful to [[PageViewApp]]
 */
object PageViewSchema {
  /**
   * Case class used as schema to read the blacklist file
   */
  case class Page(domain_code: String, page_title: String)

  /**
   * Case class used as schema to read the page views files
   */
  case class PageView(domain_code: String, page_title: String, count_views: Long)
}
