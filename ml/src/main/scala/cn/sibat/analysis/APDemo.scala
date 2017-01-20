package cn.sibat.analysis

import java.text.SimpleDateFormat
import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kong on 2017/1/11.
  */
object APDemo {
  /**
    * 过滤出大剧院的用户，并把ap对应成线路
    *
    * @param spark sparkSession
    */
  def filter(spark: SparkSession): Unit = {
    val ap = spark.sparkContext.textFile("ml/data/analysis/dajuyuanAP").collect()

    val line1Ap = "2,3,4,5,7,42,82,83,84,85,87,89,92,93,94,95,96,97,98,99".split(",")
    val line2Ap = "102,103,104,105,106,107,108,109,110,115,117,118,121,122,128,129,141,144".split(",")
    val floorAP = "6\n8\n9\n10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20\n37\n40\n43\n45\n61\n65\n74\n75\n111\n112\n113\n114\n116\n119\n120\n123\n124\n125\n126\n127\n130\n131\n132\n133\n134\n135\n136\n137\n138\n139\n140\n142\n143\n145\n146\n147\n148\n149\n150\n151\n152\n153".split("\n")

    //mac	bid	fid	aid	apid	stime	longitude	latitude
    val apData = spark.read.csv("D:/testData/AP数据/log_other_201611.csv")
    val filter = apData.filter(s => ap.contains(s.getString(4)))
    //println(filter.count()+";"+apData.count()+":"+filter.count().toDouble/apData.count().toDouble)
    val countToken = udf { (words: String) => {
      if (line1Ap.contains(words))
        "line1"
      else if (line2Ap.contains(words))
        "line2"
      else if (floorAP.contains(words))
        "floor"
      else "other"
    }
    }

    val category = filter.withColumn("category", countToken(col("_c4")))

    import spark.sqlContext.implicits._
    val result = category.select("_c0", "_c5", "category").rdd.map(s => (s.getString(0) + "," + s.getString(1).split(" ")(0), s.getString(1) + "," + s.getString(2)))
      .groupByKey().map(tuple => {
      val itr = tuple._2.iterator
      val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      var first = 0L
      var last = 0L
      val array = new util.ArrayList[String]()
      val sb = new StringBuilder
      var i = 0
      while (itr.hasNext) {
        val next = itr.next()
        val timestamp = sdf.parse(next.split(",")(0)).getTime / 1000
        val cate = next.split(",")(1)
        if (i == 0) {
          first = timestamp
          sb.append(next.split(",")(0).split(" ")(1)).append("-").append(cate)
        } else {
          last = timestamp
          if (last - first > 60 * 60) {
            array.add(sb.toString())
            sb.clear()
            sb.append(next.split(",")(0).split(" ")(1)).append("-").append(cate)
          } else {
            sb.append(",").append(cate)
          }
          first = last
        }
        i += 1
      }
      array.add(sb.toString())
      (tuple._1, array.toArray().mkString(";"))
    }).sortByKey(true).flatMap(s => {
      val xc = s._2.split(";")
      val result = new util.ArrayList[String]()
      for (x <- xc) {
        result.add(s._1.replace(",", ";") + ";" + x.replace("-", ";"))
      }
      result.toArray
    })

    result.saveAsTextFile("D:/testData/AP数据/filter")
  }

  /**
    * 对用户添加上用户类型
    * 0 unknown
    * 1 line1 进站
    * 2 line2 进站
    * 3 line1 出站
    * 4 line2 出站
    * 5 换乘
    *
    * @param sparkSession SparkSession
    */
  def addType(sparkSession: SparkSession): Unit = {
    sparkSession.sparkContext.textFile("D:/testData/AP数据/filter/p*").map(s => {
      var index = 0
      var typeIndex = 0
      if (s.contains("floor,line1")) {
        index = s.lastIndexOf("floor,line1")
        typeIndex = 1
      }

      if (s.contains("floor,line2")) {
        val temp = s.lastIndexOf("floor,line2")
        index = {
          if (index > temp) {
            index
          } else {
            typeIndex = 2
            temp
          }
        }
      }
      if (s.contains("line1,floor")) {
        val temp = s.lastIndexOf("line1,floor")
        index = {
          if (index > temp) {
            index
          } else {
            typeIndex = 3
            temp
          }
        }
      }
      if (s.contains("line2,floor")) {
        val temp = s.lastIndexOf("line2,floor")
        index = {
          if (index > temp) {
            index
          } else {
            typeIndex = 4
            temp
          }
        }
      }
      if (s.contains("line1") && s.contains("line2")) {
        typeIndex = 5
      }
      s + ";" + typeIndex
    }).saveAsTextFile("D:/testData/AP数据/addType")
  }

  /**
    * 为用户类型测算出概率
    *
    * @param sparkSession SparkSession
    */
  def probability(sparkSession: SparkSession): Unit = {
    val metaData = sparkSession.sparkContext.textFile("D:/testData/AP数据/addType/p*")
    val data = metaData.map(s => (s.split(";")(0), s))
    val typeData = metaData.map(s => (s.split(";")(0), s)).groupByKey().map(s => {
      val it = s._2.iterator
      var type1 = 0
      var type2 = 0
      var type3 = 0
      var type4 = 0
      var type5 = 0
      var total = 0
      var type1Time = new ArrayBuffer[Int]()
      var type2Time = new ArrayBuffer[Int]()
      var type3Time = new ArrayBuffer[Int]()
      var type4Time = new ArrayBuffer[Int]()
      var type5Time = new ArrayBuffer[Int]()
      while (it.hasNext) {
        val next = it.next()
        val nextType = next.split(";")(4)
        val time = next.split(";")(2).split(":")(0)
        nextType match {
          case "1" => {
            type1 += 1
            type1Time += time.toInt
          }
          case "2" => {
            type2 += 1
            type2Time += time.toInt
          }
          case "3" => {
            type3 += 1
            type3Time += time.toInt
          }
          case "4" => {
            type4 += 1
            type4Time += time.toInt
          }
          case "5" => {
            type5 += 1
            type5Time += time.toInt
          }
          case _ =>
        }
        total += 1
      }
      val sb = new StringBuilder
      sb.append(";type1:")
        .append(type1.toDouble / total.toDouble)
        .append("+")
        .append(type1Time.toArray.mkString("-"))
        .append(",type2:")
        .append(type2.toDouble / total.toDouble)
        .append("+")
        .append(type2Time.toArray.mkString("-"))
        .append(",type3:")
        .append(type3.toDouble / total.toDouble)
        .append("+")
        .append(type3Time.toArray.mkString("-"))
        .append(",type4:")
        .append(type4.toDouble / total.toDouble)
        .append("+")
        .append(type4Time.toArray.mkString("-"))
        .append(",type5:")
        .append(type5.toDouble / total.toDouble)
        .append("+")
        .append(type5Time.toArray.mkString("-"))
        .append(",total:")
        .append(total)
      (s._1, sb.toString())
    })
    data.leftOuterJoin(typeData).map(s => {
      val oldData = s._2._1
      val pro = s._2._2.orNull
      oldData + pro
    }).saveAsTextFile("D:/testData/AP数据/probability2")
  }

  /**
    * 过滤从未进入过地铁的人
    * result:(noProbability) 597281:1317067:0.4534932543295064
    * result:(hasProbability)606603:1317067:0.4605711023053497
    *
    * @param sparkSession SparkSession
    */
  def filterNo(sparkSession: SparkSession): Unit = {
    val data = sparkSession.sparkContext.textFile("D:/testData/AP数据/probability2/p*")
    val t = data.filter(s => {
      var result = false //计算没有概率是设为true
      val split = s.split(";")(5).split(",")
      for (i <- 0 until split.length - 1) {
        if (split(i).split("[:+]")(1).toDouble > 0.0)
          result = true //计算没有概率是设为false
      }
      result
    })
    //过滤没有概率时，丢弃掉的条件
    //      .map(s => (s.split(";")(0), s)).groupByKey().filter(s => {
    //      val s2 = s._2.mkString("+")
    //      s2.contains("line1") || s2.contains("line2")
    //    }).flatMap(s => s._2)
    t.saveAsTextFile("D:/testData/AP数据/hasProbability2")
    //    val f = data.count()
    //    println(t.count() + ":" + f + ":" + t.count().toDouble / f.toDouble)
  }

  /**
    * 没有概率里面识别出潮汐运动的人，根据时间线进行平滑
    *
    * @param sparkSession SparkSession
    */
  def lawOfNoProbability(sparkSession: SparkSession): Unit = {
    val data = sparkSession.sparkContext.textFile("D:/testData/AP数据/noProbability/p*").map(s => (s.split(";")(0), s))
    val right = data.groupByKey().map(s => {
      val it = s._2.iterator
      var morning = 0
      var evening = 0
      val mornings = Array("7", "8", "9", "10")
      val evenings = Array("17", "18", "19", "20", "21", "22", "23")
      var total = 0
      while (it.hasNext) {
        val data = it.next()
        val hour = data.split(";")(2).split(":")(0)
        val temp = {
          if (mornings.contains(hour))
            "morning"
          else if (evenings.contains(hour))
            "evening"
          "other"
        }
        temp match {
          case "morning" => morning += 1
          case "evening" => evening += 1
          case _ =>
        }
        total += 1
      }
      val bl = morning.toDouble / evening.toDouble
      var result = false
      if (bl >= 0.8 && bl <= 1.2)
        result = true
      (s._1, result)
    })

    data.leftOuterJoin(right).map(s => s._2._1 + ";" + s._2._2.getOrElse(false)).saveAsTextFile("D:/testData/AP数据/NPCX")
  }

  /**
    * 不能平滑的数据，在地铁里面的ap个数大于3个，分为有行为能力用户
    * 和经过用户
    *
    * @param sparkSession SparkSession
    */
  def hasAbility(sparkSession: SparkSession): Unit = {
    val count = sparkSession.sparkContext.textFile("D:/testData/AP数据/noCause/p*").filter(s => {
      val split = s.split(";")
      split(3).split(",").length <= 3 || split(3).contains("floor")
    }) //.count()
    //println(count)
    count.saveAsTextFile("D:/testData/AP数据/noAbility")
  }

  /**
    * 预测所属类型
    *
    * @param hour  发生时间
    * @param hours 类型时间集合串
    * @return 所属类型
    */
  def predict(hour: Int, hours: String): Int = {
    val split = hours.split(",")
    var tmp = 0.0
    var target = 0
    for (i <- 0 until split.length - 1) {
      val tpt = split(i).split("[:+]")
      if (tpt.length > 2) {
        val sorted = tpt(2).split("-").map(s => math.abs(hour - s.toInt)).sorted
        val pro = (24 - sorted(0)).toDouble / 24.0 //tpt(1).toDouble * ((24 - sorted(0)).toDouble / 24.0)
        if (pro > tmp) {
          target = i + 1
          tmp = pro
        }
      }
    }
    target
  }

  /**
    * 处理有概率事件
    *
    * @param sparkSession SparkSession
    */
  def dealHasProbability(sparkSession: SparkSession): Unit = {
    sparkSession.sparkContext.textFile("D:/testData/AP数据/hasProbability2/p*").map(s => {
      val split = s.split(";")
      val sb = new StringBuilder
      val hour = split(2).split(":")(0).toInt
      if (split(4).equals("0")) {
        val pre = predict(hour, split(5))
        split(4) = pre.toString
      }
      for (str <- split) {
        sb.append(str).append(";")
      }
      sb.toString()
    }).saveAsTextFile("D:/testData/AP数据/ProResult2")
  }

  /**
    * 从没有概率中识别出因果出行的行程
    */
  def cause(sparkSession: SparkSession): Unit = {
    val data = sparkSession.sparkContext.textFile("D:/testData/AP数据/noProbability/p*")
    val cause = data.map(s => {
      val split = s.split(";")
      (split(0) + ";" + split(1), 1)
    }).reduceByKey(_ + _).filter(_._2 % 2 == 0)
    val causeData = data.map(s => (s.split(";")(0) + ";" + s.split(";")(1), s)).join(cause).map(s => s._2._1)
    //causeData.saveAsTextFile("D:/testData/AP数据/cause")
    val noCauseData = data.map(s => (s.split(";")(0) + ";" + s.split(";")(1), s)).leftOuterJoin(cause).filter(s => s._2._2.getOrElse(0) == 0).map(s => s._2._1)
    noCauseData.saveAsTextFile("D:/testData/AP数据/noCause")
    println(causeData.count() + ":" + noCauseData.count())
  }

  /**
    * 对ability数据进行归类
    *
    * @param sparkSession SparkSession
    */
  def abilityPredict(sparkSession: SparkSession): Unit = {
    sparkSession.sparkContext.textFile("D:/testData/AP数据/ability/").map(s => {
      val split = s.split(";")
      if (split(3).contains("line1")) {
        split(4) = "1"
      } else
        split(4) = "2"
      split.mkString(";")
    }).saveAsTextFile("D:/testData/AP数据/abilityResult")
  }

  /**
    * 因果出行结果一半一半
    *
    * @param sparkSession SparkSession
    */
  def causePredict(sparkSession: SparkSession): Unit = {
    sparkSession.sparkContext.textFile("D:/testData/AP数据/cause").map(s => (s.split(";")(0), s)).groupByKey().flatMap(s => {
      val it = s._2.iterator
      val result = new ArrayBuffer[String]()
      val temp = new ArrayBuffer[String]()
      var tmp = "0"
      while (it.hasNext) {
        val line = it.next()
        val split = line.split(";")
        if (split(3).contains("line1")) {
          split(4) = "1or3"
          tmp = split(4)
        } else if (split(3).contains("line2")) {
          split(4) = "2or4"
          tmp = split(4)
        }
        temp += split.mkString(";")
      }
      for (r <- temp) {
        val split = r.split(";")
        if (split(3).contains("floor"))
          split(4) = tmp
        result += split.mkString(";")
      }
      result
    }).saveAsTextFile("D:/testData/AP数据/causeResult")
  }

  /**
    * 结果统计
    * result：(1,318042);(2,81985);(3,265037);(4,70160);(5,57339);
    * @param sparkSession SparkSession
    */
  def resultCount(sparkSession: SparkSession): Unit = {
    val data1 = sparkSession.sparkContext.textFile("D:/testData/AP数据/causeResult")
    val data2 = sparkSession.sparkContext.textFile("D:/testData/AP数据/abilityResult")
    val data3 = sparkSession.sparkContext.textFile("D:/testData/AP数据/ProResult2")
    val result = data1.union(data2).union(data3).map(s => (s.split(";")(4), 1)).reduceByKey(_ + _).collect()

    println(result.mkString(";"))
    for (r <- result) {
      if (r._1.equals("1or3")) {
        result.update(1, (result(1)._1, result(1)._2 + r._2 / 2))
        result.update(3, (result(3)._1, result(3)._2 + r._2 - r._2 / 2))
      }
      if (r._1.equals("2or4")) {
        result.update(2, (result(2)._1, result(2)._2 + r._2 / 2))
        result.update(4, (result(4)._1, result(4)._2 + r._2 - r._2 / 2))
      }
    }
    println(result.mkString(";"))
  }

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:/download/hadoop_2.6.0_64bit")
    val spark = SparkSession.builder().appName("APDemo").master("local[*]").getOrCreate()
    spark.sparkContext.textFile("D:/testData/AP数据/filter").map(s=>(s.split(";")(1),1)).reduceByKey(_+_).sortByKey().map(s=>s._1+";"+s._2).repartition(1).saveAsTextFile("D:/testData/AP数据/cdf")
    //resultCount(spark)
    /**
      * 0.4012829264045887
0.10344288088139365
0.3344049621291935
0.08852293130009854
0.07234629928472563
792563
      */
//    val array = Array(318042,81985,265037,70160,57339)
//    val total = array.sum
//    for (a <- array ) {
//      println(a.toDouble/total.toDouble)
//    }
//    println(total)
    //    val result = spark.sparkContext.textFile("D:/testData/AP数据/filter/p*")
    //    val have = result.filter(s=> s.contains("line1,floor") || s.contains("floor,line1") || s.contains("line2,floor") || s.contains("floor,line2") || s.contains("line1,line2") || s.contains("line2,line1"))
    //      .count()
    //
    //    println(have+":"+have.toDouble/result.count().toDouble)
    //spark.close()
  }
}
