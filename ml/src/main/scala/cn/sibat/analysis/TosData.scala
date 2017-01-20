package cn.sibat.analysis

import java.text.SimpleDateFormat

import cn.sibat.util.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * tos 数据处理
  * Created by kong on 2017/1/19.
  */
object TosData {
  /**
    * 初始记录转化为目标记录格式
    * Cardid,trade_time,trade_address,trade_type,receive_time
    *
    * @param spark SparkSession
    */
  def dealDealt(spark: SparkSession): Unit = {
    spark.sparkContext.textFile("D:/testData/tosData/realt*").map(s => {
      val split = s.split("\t")
      split(4) + "," + split(5) + "," + split(3) + "," + split(2) + "," + split(7)
    }).saveAsTextFile("D:/testData/tosData/newData")
  }

  /**
    * 连接成一个总表
    *
    * @param spark SparkSession
    */
  def deal(spark: SparkSession): Unit = {
    val df = spark.read.csv("D:/testData/tosData/data/")
    val countTime = udf { (tradeTime: String, receiveTime: String) => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      val receiveSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (receiveSDF.parse(receiveTime).getTime - sdf.parse(tradeTime).getTime) / 1000
    }
    }

    val timeToHour = udf { (tradeTime: String) => {
      tradeTime.split(":")(0)
    }
    }

    val data = df.withColumnRenamed("_c0", "cardId")
      .withColumnRenamed("_c1", "tradeTime")
      .withColumnRenamed("_c2", "tradeAddress")
      .withColumnRenamed("_c3", "tradeType")
      .withColumnRenamed("_c4", "receiveTime")
      .distinct().withColumn("difference", countTime(col("tradeTime"), col("receiveTime"))) //.filter(col("difference") >= 1200)
    val stationInfo = spark.read.csv("D:/testData/tosData/code1.csv").withColumnRenamed("_c0", "tradeAddress").withColumnRenamed("_c1", "name").withColumnRenamed("_c2", "code").withColumnRenamed("_c3", "line")
    val target = data.join(stationInfo, "tradeAddress").withColumn("timeLine", timeToHour(col("tradeTime")))
    //target.rdd.saveAsTextFile("D:/testData/tosData/newResult")
    target.persist(StorageLevel.MEMORY_AND_DISK)
    target.select("line").rdd.map(s => (s.getString(0), 1)).reduceByKey(_ + _).repartition(1).saveAsTextFile("D:/testData/tosData/lineTotal")
    target.select("name").rdd.map(s => (s.getString(0), 1)).reduceByKey(_ + _).repartition(1).saveAsTextFile("D:/testData/tosData/stationTotal")
    target.select("timeLine").rdd.map(s => (s.getString(0), 1)).reduceByKey(_ + _).repartition(1).saveAsTextFile("D:/testData/tosData/timeTotal")
    target.unpersist()
  }

  def statistics(spark: SparkSession): Unit = {
    val data1 = spark.sparkContext.textFile("D:/testData/tosData/newResult")
    val data2 = spark.sparkContext.textFile("D:/testData/tosData/result")
    val data = data1.union(data2).distinct()
    data.map(s => {
      val split = s.split(",")
      (split(8), 1)
    }).reduceByKey(_ + _).repartition(1).saveAsTextFile("D:/testData/tosData/line")
    data.map(s => {
      val split = s.split(",")
      (split(6), 1)
    }).reduceByKey(_ + _).repartition(1).saveAsTextFile("D:/testData/tosData/station")
    data.map(s => {
      val split = s.split(",")
      (split(9).replace("]", ""), 1)
    }).reduceByKey(_ + _).repartition(1).saveAsTextFile("D:/testData/tosData/time")
  }

  def join(spark:SparkSession): Unit ={
    val line = spark.sparkContext.textFile("D:/testData/tosData/line.txt")
    val station = spark.sparkContext.textFile("D:/testData/tosData/station.txt")
    val time = spark.sparkContext.textFile("D:/testData/tosData/time.txt")
    val lineRight = spark.sparkContext.textFile("D:/testData/tosData/lineTotal.txt")
    val stationRight = spark.sparkContext.textFile("D:/testData/tosData/stationTotal.txt")
    val timeRight = spark.sparkContext.textFile("D:/testData/tosData/timeTotal.txt")
    line.map(s=>(s.split(",")(0),s.split(",")(1))).join(lineRight.map(s=>(s.split(",")(0),s.split(",")(1)))).map(s=>s._1+","+s._2._1+","+s._2._2+","+s._2._1.toDouble/s._2._2.toDouble).repartition(1).saveAsTextFile("D:/testData/tosData/lineResult")
    station.map(s=>(s.split(",")(0),s.split(",")(1))).join(stationRight.map(s=>(s.split(",")(0),s.split(",")(1)))).map(s=>s._1+","+s._2._1+","+s._2._2+","+s._2._1.toDouble/s._2._2.toDouble).repartition(1).saveAsTextFile("D:/testData/tosData/stationResult")
    time.map(s=>(s.split(",")(0),s.split(",")(1))).join(timeRight.map(s=>(s.split(",")(0),s.split(",")(1)))).map(s=>s._1+","+s._2._1+","+s._2._2+","+s._2._1.toDouble/s._2._2.toDouble).repartition(1).saveAsTextFile("D:/testData/tosData/timeResult")
  }

  def main(args: Array[String]) {
    val spark = SparkUtils.session("TosData")
    join(spark)
    spark.close()
  }
}
