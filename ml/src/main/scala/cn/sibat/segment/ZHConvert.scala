package cn.sibat.segment

import java.io.{BufferedReader, InputStreamReader}
import java.util
import java.util.Properties

import scala.tools.nsc.interpreter.InputStream

/**
  * 中文简繁转换,单例写法
  * Created by kong on 2016/12/29.
  */
class ZHConvert private(prop: String) extends Serializable {

  //配置文件，简繁对应表
  private val charMap = new Properties()
  private val conflictingSets = new util.HashSet[String]

  /**
    * 初始化，加载配置文件
    * @return
    */
  private def apply(): this.type = {
    val is: InputStream = getClass.getClassLoader.getResourceAsStream(prop)
    if (is != null) {
      var reader: BufferedReader = null
      try {
        reader = new BufferedReader(new InputStreamReader(is))
        charMap.load(reader)
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (reader != null)
          reader.close()
        is.close()
      }
    }
    initializeHelper()
    this
  }

  private def initializeHelper() = {
    val stringPossibilities = new util.HashMap[String, Int]()
    val it = charMap.keySet().iterator()
    while (it.hasNext) {
      val key = classOf[String].cast(it.next())
      if (key.length >= 1) {
        for (i <- 0 until key.length) {
          val keySubstring = key.substring(0, i + 1)
          if (stringPossibilities.containsKey(keySubstring)) {
            val value = stringPossibilities.get(keySubstring)
            stringPossibilities.put(keySubstring, value + 1)
          } else
            stringPossibilities.put(keySubstring, 1)
        }
      }
    }
    val itr = stringPossibilities.keySet().iterator()
    while (itr.hasNext) {
      val key = itr.next()
      if (stringPossibilities.get(key) > 1)
        conflictingSets.add(key)
    }
  }

  def convert(text: String): String = {
    val outString = new StringBuilder
    val stackString = new StringBuilder
    for (i <- 0 until text.length) {
      val c = text.charAt(i)
      val key = "" + c
      stackString.append(key)
      if (conflictingSets.contains(stackString.toString())) {}
      else if (charMap.containsKey(stackString.toString())) {
        outString.append(charMap.get(stackString.toString()))
        stackString.setLength(0)
      } else {
        val sequence = stackString.subSequence(0, stackString.length - 1)
        stackString.delete(0, stackString.length - 1)
        flushStack(outString, new StringBuilder().append(sequence))
      }
    }
    flushStack(outString, stackString)
    outString.toString()
  }

  private def flushStack(outString: StringBuilder, stackString: StringBuilder): Unit = {
    while (stackString.nonEmpty) {
      if (charMap.containsKey(stackString.toString())) {
        outString.append(charMap.get(stackString.toString()))
        stackString.setLength(0)
      } else {
        outString.append("").append(stackString.charAt(0))
        stackString.delete(0, 1)
      }
    }
  }
}

object ZHConvert {
  private var zHConvert: ZHConvert = _
  private val TRADITIONAL = 0
  private val SIMPLIFIED = 1
  private val propertyFiles = Array("zh2t.properties", "zh2s.properties")

  def getInstance(convertType: Int): ZHConvert = if (zHConvert == null) {
    require(convertType >= 0 && convertType < 2, s"convertType $convertType not in 0,1")
    zHConvert = new ZHConvert(propertyFiles(convertType)).apply()
    zHConvert
  } else zHConvert

  def convert(text: String, convertType: Int): String = {
    getInstance(convertType).convert(text)
  }
}
