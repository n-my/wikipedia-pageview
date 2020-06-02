package wikipedia

import org.scalatest.{FunSuite, Matchers}
class UsageConfigTest extends FunSuite with Matchers {
  test("valid date") {
    val date = "2020-06-01-23"
    val result = new UsageOptionParser().validateDate(date)
    result match {
      case Right(()) => succeed
      case _ => fail
    }
  }
  test("invalid date") {
    val date = "2020-06-01-25"
    val result = new UsageOptionParser().validateDate(date)
    result match {
      case Left(s) => s should startWith ("Invalid date.")
      case _ => fail
    }
  }
  test("valid bucket name") {
    val name = "dd-interview-data"
    val result = new UsageOptionParser().validateBucketName(name)
    result match {
      case Right(()) => succeed
      case _ => fail(s"${name} should be valid")
    }
  }
  test("invalid bucket names") {
    val invalidNames = Seq("", "MyBucket", "mybucket-", "my..bucket")
    val optionParser = new UsageOptionParser()
    invalidNames.foreach { name =>
      val result = optionParser.validateBucketName(name)
      result match {
        case Left(s) => s should equal ("Invalid bucket name.")
        case _ => fail
      }
    }
  }
}
