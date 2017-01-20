package cn.sibat.segment

import java.util
import java.util.Properties

import com.hankcs.hanlp.HanLP
import org.ansj.domain.Term
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * 数据预处理类，包含一下操作：
  * 基本清洗（繁简转换、全半角转换、去除无意义词）、分词、分句、去除停用词、去除低频词
  * Created by kong on 2016/12/29.
  */
class SegmentUtils(config: SegmentConfig) extends Serializable {

  //英文字符正则
  private val enExpr = "[A-Za-z]".r
  //数字正则
  private val numExpr = "\\d+(\\.\\d+)?(\\/\\d+)?".r
  //匹配英文字母、数字、中文之外的字符
  private val baseExpr =
    """[^\w-\s+\u4e00-\u9fa5]""".r

  private val zhConvert = ZHConvert.getInstance(1)

  /**
    * 封装sc.textFile方法,可以按指定的最小块切分读取
    *
    * @param sc      sparkContext
    * @param inPath  输入路径
    * @param minSize 最小块大小 默认32
    * @return RDD[String]
    */
  def getTextFile(sc: SparkContext, inPath: String, minSize: Int = 32): RDD[String] = {
    val hadoopConf = sc.hadoopConfiguration
    val fs = new Path(inPath).getFileSystem(hadoopConf)
    //获取数据大小，以MB为单位
    val len = fs.getContentSummary(new Path(inPath)).getLength / (1024 * 1024)
    //按最小块进行切分
    val minPart = (len / minSize).toInt
    sc.textFile(inPath, minPart)
  }

  /**
    * 全角转半角
    *
    * @param line 输入数据
    * @return
    */
  def q2b(line: String): String = {
    BCConvert.q2b(line)
  }

  /**
    * 繁体字转简体字
    *
    * @param line 输入数据
    * @return 简体字
    */
  def t2s(line: String): String = {
    HanLP.convertToSimplifiedChinese(line)
  }

  /**
    * 简体字转繁体字
    *
    * @param line 输入数据
    * @return 繁体字
    */
  def s2t(line: String): String = {
    HanLP.convertToTraditionalChinese(line)
  }

  /**
    * 清洗一行数据
    * 基础处理，包括繁体转简体、全角转半角、去除不可见字符、数值替换、去英文字符
    *
    * @param line 数据数据
    * @return 清洗后数据
    */
  def baseClean(line: String): String = {
    var result = line.trim
    val numToChar = config.numToChar

    //繁体转简体
    if (config.t2s) {
      result = t2s(result)
    }

    //全角转半角
    if (config.q2b)
      result = q2b(result)

    //去除不可见字符
    result = baseExpr.replaceAllIn(result, "")
    result = StringUtils.trimToEmpty(result)

    //替换数字
    if (config.delNum)
      result = enExpr.replaceAllIn(result, numToChar)

    //去除英文字字符
    if (config.delEn)
      result = enExpr.replaceAllIn(result, "")
    result
  }

  /**
    * 使用ansj分词工具进行分词
    *
    * @param text          文本
    * @param stopWordArray 停用词
    * @return 分词结果
    */
  def ansjSegment(text: String, stopWordArray: Array[String]): util.List[Term] = {
    val filter = new StopRecognition()
    for (stopWord <- stopWordArray) {
      filter.insertStopWords(stopWord)
    }
    val splitType = config.splitType
    val result = splitType match {
      case "BaseAnalysis" => BaseAnalysis.parse(text).recognition(filter)
      case "ToAnalysis" => ToAnalysis.parse(text).recognition(filter)
      case "DicAnalysis" => DicAnalysis.parse(text).recognition(filter)
      case "IndexAnalysis" => IndexAnalysis.parse(text).recognition(filter)
      case "NlpAnalysis" => NlpAnalysis.parse(text).recognition(filter)
      case _ =>
        println("分词方式不对，请检查splitType")
        sys.exit(1)
    }
    result.getTerms
  }

  /**
    * 分词，每行返回一个Seq[String]的分词结果
    *
    * @param text          输入文本
    * @param stopWordArray 停用词
    * @return 分词结果
    */
  def wordSegment(text: String, stopWordArray: Array[String]): Option[Seq[String]] = {
    var arrayBuffer = ArrayBuffer[String]()
    val splitTool = config.splitTool
    if (text != null && text != "") {
      val tmp = new util.ArrayList[Term]()
      splitTool match {
        case "ansj" =>
          val result: util.List[Term] = ansjSegment(text, stopWordArray)
          tmp.addAll(result)
        case _ =>
          println("分词工具错误，请检查splitTool。或者自行增加（cn.sibat.segment.wordSegment()）")
          sys.exit(1)
      }

      val addNature = config.addNature
      for (i <- 0 until tmp.size()) {
        val term = tmp.get(i)
        if (addNature) {
          val item = term.getName
          val nature = term.getNatureStr
          arrayBuffer += item + "/" + nature
        } else {
          var item = term.getName.trim
          arrayBuffer += item
        }
      }
      Some(arrayBuffer)
    } else {
      None
    }
  }

  /**
    * 分段，对文本按照指定的分隔符分段
    *
    * @param content 输入的一行数据
    * @param sep     分割符
    * @return 分割完数组
    */
  def paragraphSegment(content: String, sep: String): Array[String] = {
    val result = new ArrayBuffer[String]()
    val paragraphs = content.split(sep)
    for (paragraph <- paragraphs) {
      val filterParagraph = paragraph.trim
      if (filterParagraph != null && filterParagraph != "") {
        result += filterParagraph
      }
    }
    result.toArray
  }

  /**
    * 获取低频词
    *
    * @param wordRDD 词序列 RDD
    * @return 低频词数组
    */
  def getRareTerms(wordRDD: RDD[(Long, scala.Seq[String])]): Array[String] = {
    val rareTermNum = config.rareTermNum
    val wc = wordRDD.flatMap(words => words._2).map((_, 1)).reduceByKey(_ + _)
    val result = wc.filter(word => word._2 < rareTermNum).map(_._1)
    result.collect()
  }

  /**
    * 删除低频词
    *
    * @param id    输入词序列编号
    * @param words 输入词序列
    * @param rares 低频词数组
    * @return 删除低频词后的词序列
    */
  def delRareTerm(id: Long, words: Seq[String], rares: Array[String]): (Long, scala.Seq[String]) = {
    val result = new ArrayBuffer[String]()
    val wordArray = words.toArray
    for (word <- wordArray) {
      if (!rares.contains(word))
        result += word
    }
    (id, result)
  }

  /**
    * 分词主函数
    * 执行分词，返回一个Seq[String]的RDD数据
    *
    * @param data 输入数据
    * @return
    */
  def run(data: RDD[(Long, String)]): RDD[(Long, Seq[String])] = {
    val sc = data.sparkContext
    val stopWordArray = sc.textFile(config.stopWordPath).collect()
    val stopWordBC = sc.broadcast(stopWordArray)

    //清洗数据
    val cleanedRDD = data.map(str => (str._1, baseClean(str._2)))

    //分词，去除停用词
    var resultRDD = cleanedRDD.map {
      line =>
        (line._1, wordSegment(line._2, stopWordBC.value))
    }.filter(_._2.nonEmpty).map(line => (line._1, line._2.get))

    stopWordBC.unpersist()

    //去除低频词
    if (config.delRareTerm) {
      val rareArray = getRareTerms(resultRDD)
      resultRDD = resultRDD.map(w => delRareTerm(w._1, w._2, rareArray))
    }

    //根据词长度过滤
    resultRDD = resultRDD.map {
      case (id: Long, value: Seq[String]) =>
        (id, value.filter(_.length >= config.minTermSize))
    }

    resultRDD
  }


}

object SegmentUtils {
  def apply(): SegmentUtils = {
    SegmentUtils("D:/workspace/idea/nlp/ml/src/main/resources/segment.properties")
  }

  def apply(conf: String): SegmentUtils = {
    val config = SegmentConfig(conf)
    new SegmentUtils(config)
  }

  def apply(prop: Properties): SegmentUtils = {
    val config = SegmentConfig(prop)
    new SegmentUtils(config)
  }

  def apply(conf: SegmentConfig): SegmentUtils = {
    new SegmentUtils(conf)
  }
}