package wikipedia

object PageViewSchema {
  case class Page(domain_code: String, page_title: String)
  case class PageView(domain_code: String, page_title: String, count_views: Long)
}

