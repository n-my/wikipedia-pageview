package wikipedia

import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{LocalDateTime, ZoneOffset}

import scopt.OptionParser

/**
 * Trait for job configuration
 */
trait Config

/**
 * Case class that holds the configuration of the [[PageViewApp]] job.
 */
case class UsageConfig(startDate: Option[LocalDateTime] = None,
                       endDate: Option[LocalDateTime] = None,
                       bucketName: String = "") extends Config

/**
 * Class that defines, parses and validates the arguments of the [[PageViewApp]] job.
 */
class UsageOptionParser
  extends OptionParser[UsageConfig](PageViewApp.appName) {
  head("scopt", "3.x")
  private val inputDatePattern = "yyyy-MM-dd-HH"
  private val dateFormatter = DateTimeFormatter.ofPattern(inputDatePattern)

  opt[String]('s', "start-date")
    .required().withFallback(() => LocalDateTime.now(ZoneOffset.UTC).minusDays(1).format(dateFormatter))
    .validate(date => validateDate(date))
    .action((value, arg) => {
      arg.copy(startDate = Some(LocalDateTime.parse(value, dateFormatter)))
    })
    .text(s"The (included) start date as ${inputDatePattern} in UTC. Defaults to the previous day and hour")

  opt[String]('e', "end-date")
    .required().withFallback(() => "")
    .validate {
      case "" => success
      case value: Any => validateDate(value)
    }
    .action((value, arg) => {
      value match {
        case "" => arg.copy(endDate = Some(arg.startDate.get.plusHours(1)))
        case _ => arg.copy(endDate = Some(LocalDateTime.parse(value, dateFormatter)))
      }
    })
    .text(s"The (excluded) end date as ${inputDatePattern} in UTC. Defaults to start-date + 1h")

  opt[String]('b', "bucket-name")
    .validate(name => validateBucketName(name))
    .required()
    .action((value, arg) => {
      arg.copy(bucketName = value)
    })
    .text("The output S3 bucket name. Required.")

  /**
   * Validates that a [[String]] can be parsed as a [[LocalDateTime]]
   * @param date the date to parse
   * @return [[success]] if validated, [[failure]] otherwise, according to the scopt `validate()` expectations
   */
  def validateDate(date: String): Either[String, Unit] = {
    try {
      LocalDateTime.parse(date, dateFormatter)
      success
    } catch {
      case e: DateTimeParseException => failure(s"Invalid date. ${e.getMessage}")
    }
  }

  /**
   * Validates a bucket name according to the AWS requirements
   * @param bucketName the bucket name
   * @return [[success]] if validated, [[failure]] otherwise, according to the scopt `validate()` expectations
   */
  def validateBucketName(bucketName: String): Either[String, Unit] = {
    val namePattern = "(?!^(\\d{1,3}\\.){3}\\d{1,3}$)(^[a-z0-9]([a-z0-9-]*(\\.[a-z0-9])?)*$(?<!\\-))".r
    namePattern.findFirstIn(bucketName) match {
      case Some(s) => success
      case None => failure("Invalid bucket name.")
    }
  }
}
