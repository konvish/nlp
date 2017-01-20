package cn.sibat.ml

import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/1/5.
  */
object LDAExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local").appName("LDA").getOrCreate()
    val dataset = spark.read.format("libsvm").load("C:/kong/spark-2.0.0-bin-hadoop2.6/data/mllib/sample_lda_libsvm_data.txt")
    dataset.rdd.foreach(println(_))
    dataset.printSchema()
  }
}
