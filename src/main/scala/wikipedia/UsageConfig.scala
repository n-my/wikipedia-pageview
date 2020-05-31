package wikipedia

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import scopt.OptionParser

trait Config

case class UsageConfig(startDate: Option[LocalDateTime] = None,
                       endDate: Option[LocalDateTime] = None,
                       bucketName: String = "") extends Config

class UsageOptionParser
  extends OptionParser[UsageConfig]("Spark job") {
  head("scopt", "3.x")
  private val inputDatePattern = "yyyy-MM-dd-HH"
  private val dateFormatter = DateTimeFormatter.ofPattern(inputDatePattern)

  opt[String]('s', "start-date")
    .required().withFallback(() => LocalDateTime.now().minusDays(1).format(dateFormatter))
    .validate(date => validateDate(date))
    .action((value, arg) => {
      arg.copy(startDate = Some(LocalDateTime.parse(value, dateFormatter)))
    })
    .text(s"The (included) start date as ${inputDatePattern}. Default to the previous day and hour")

  opt[String]('e', "end-date")
    .required().withFallback(() => "")
    .validate {
      case "" => success
      case value => validateDate(value)
    }
    .action((value, arg) => {
      value match {
        case "" => arg.copy(endDate = Some(arg.startDate.get.plusHours(1)))
        case _ => arg.copy(endDate = Some(LocalDateTime.parse(value, dateFormatter)))
      }
    })
    .text(s"The (excluded) end date as ${inputDatePattern}. Default to start-date + 1h")

  opt[String]('b', "bucket-name")
    .validate(name => validateBucketName(name))
    .required()
    .action((value, arg) => {
      arg.copy(bucketName = value)
    })
    .text("The S3 bucket name")

  private def validateDate(date: String) = {
    try {
      LocalDateTime.parse(date, dateFormatter)
      success
    } catch {
      case e: DateTimeParseException => failure(s"Invalid date. ${e.getMessage}")
    }
  }

  private def validateBucketName(bucketName: String) = {
    val namePattern = "(?!^(\\d{1,3}\\.){3}\\d{1,3}$)(^[a-z0-9]([a-z0-9-]*(\\.[a-z0-9])?)*$(?<!\\-))".r
    namePattern.findFirstIn(bucketName) match {
      case Some(s) => success
      case None => failure("Invalid bucket name.")
    }
  }
}
