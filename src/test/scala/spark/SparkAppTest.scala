package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
 * Trait used to create Scala tests that requires a [[SparkSession]]
 * or a [[Storage]]
 */
trait SparkAppTest extends FunSuite {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }
  def sparkConf: SparkConf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("Unit testing")
  }
  def sc: SparkContext = {
    spark.sparkContext
  }
  def storage: Storage = {
    new Storage(spark)
  }
}
