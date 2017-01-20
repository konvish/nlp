package cn.sibat.segment

import java.io.{BufferedReader, InputStreamReader, FileInputStream}
import java.util.Properties

import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.math.NumberUtils

/**
  * 数据预处理配置类
  * Created by kong on 2016/12/29.
  */
case class SegmentConfig(t2s: Boolean, q2b: Boolean, delNum: Boolean, numToChar: String, delEn: Boolean
                         , delStopWord: Boolean, splitWord: Boolean, splitTool: String, splitType: String
                         , addNature: Boolean, oneGram: Boolean, minTermSize: Int, minTermNum: Int, delRareTerm: Boolean
                         , rareTermNum: Int, toParagraphs: Boolean, paragraphSeparator: String, stopWordPath: String)

object SegmentConfig {
  /**
    * 参数列表
    */
  val PARAMS = List("t2s", "q2b", "delNum", "numToChar", "delEn", "delStopWord", "splitWord", "splitTool", "splitType"
    , "addNature", "oneGram", "minTermSize", "minTermNum", "delRareTerm", "rareTermNum", "toParagraphs", "paragraphSeparator", "stopWordPath")

  /**
    * 根据配置文件生成配置对象SegmentConfig
    *
    * @param prop 参数配置
    * @return SegmentConfig
    */
  def apply(prop: Properties): SegmentConfig = {
    checkParams(prop)
    val t2s = BooleanUtils.toBoolean(if (prop.getProperty("t2s") == null) "true" else prop.getProperty("t2s"))
    val q2b = BooleanUtils.toBoolean(if (prop.getProperty("q2b") == null) "true" else prop.getProperty("q2b"))
    val delNum = BooleanUtils.toBoolean(if (prop.getProperty("delNum") == null) "false" else prop.getProperty("delNum"))
    val numToChar = prop.getProperty("numToChar", "")
    val delEn = BooleanUtils.toBoolean(if (prop.getProperty("delEn") == null) "false" else prop.getProperty("delEn"))
    val delStopWord = BooleanUtils.toBoolean(if (prop.getProperty("delStopWord") == null) "false" else prop.getProperty("delStopWord"))
    val splitWord = BooleanUtils.toBoolean(if (prop.getProperty("splitWord") == null) "false" else prop.getProperty("splitWord"))
    val splitTool = prop.getProperty("splitTool", "ansj")
    val splitType = prop.getProperty("splitType", "ToAnalysis")
    val addNature = BooleanUtils.toBoolean(if (prop.getProperty("addNature") == null) "false" else prop.getProperty("addNature"))
    val oneGram = BooleanUtils.toBoolean(if (prop.getProperty("oneGram") == null) "false" else prop.getProperty("oneGram"))
    val minTermSize = NumberUtils.toInt(prop.getProperty("minTermSize"), 1)
    val minTermNum = NumberUtils.toInt(prop.getProperty("minTermNum"), 10)
    val delRareTerm = BooleanUtils.toBoolean(if (prop.getProperty("delRareTerm") == null) "false" else prop.getProperty("delRareTerm"))
    val rareTermNum = NumberUtils.toInt(prop.getProperty("rareTermNum"), 1)
    val toParagraphs = BooleanUtils.toBoolean(if (prop.getProperty("toParagraphs") == null) "false" else prop.getProperty("toParagraphs"))
    val paragraphSeparator = if (prop.getProperty("paragraphSeparator") == null) "		" else prop.getProperty("paragraphSeparator").replaceAll("<|>", "")
    val stopWordPath = prop.getProperty("stopWordPath", "")

    new SegmentConfig(t2s, q2b, delNum, numToChar, delEn, delStopWord, splitWord, splitTool, splitType, addNature, oneGram, minTermSize, minTermNum, delRareTerm, rareTermNum, toParagraphs, paragraphSeparator, stopWordPath)
  }

  /**
    * 读取配置文件初始化配置类
    *
    * @param propPath 配置文件路径
    * @return SegmentConfig
    */
  def apply(propPath: String): SegmentConfig = {
    val prop = new Properties()
    try {
      prop.load(new BufferedReader(new InputStreamReader(new FileInputStream(propPath), "UTF-8")))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    SegmentConfig(prop)
  }

  /**
    * 从key=value格式的数组初始化配置类
    * @param kvs kv数值
    * @return SegmentConfig
    */
  def apply(kvs: Array[String]): SegmentConfig = {
    val prop = new Properties()
    kvs.foreach {
      kv => {
        val temp = kv.split("=")
        prop.setProperty(temp(0), temp(1))
      }
    }
    SegmentConfig(prop)
  }

  /**
    * 检查参数
    *
    * @param prop 配置文件
    */
  def checkParams(prop: Properties): Unit = {
    val keyItr = prop.keySet().iterator()
    while (keyItr.hasNext) {
      val key = keyItr.next()
      if (!PARAMS.contains(key)) {
        throw new Exception(s"unknown param: $key ,please delete it.")
      }
    }
  }
}