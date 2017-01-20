package cn.sibat.util

import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/1/19.
  */
object SparkUtils {
  def session(name: String): SparkSession = {
    session(name, "local[*]")
  }

  def session(name: String, master: String): SparkSession = {
    SparkSession.builder().appName(name).master(master).getOrCreate()
  }
}
