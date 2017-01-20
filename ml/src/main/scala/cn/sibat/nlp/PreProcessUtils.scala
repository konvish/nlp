package cn.sibat.nlp

import cn.sibat.segment.SegmentUtils
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{CountVectorizerModel, IDFModel, LabeledPoint}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * 预处理函数，主要对分词进行向量化
  * Created by kong on 2017/1/3.
  */
class PreProcessUtils(private var blockSize: Int, private var minDocFreq: Int, private var toTFIDF: Boolean, private var vocabSize: Int) {
  def this() = this(blockSize = 48, minDocFreq = 2, toTFIDF = true, vocabSize = 5000)

  def setBlockSize(blockSize: Int): this.type = {
    require(minDocFreq > 0, "切分块大小必须大于0")
    this.blockSize = blockSize
    this
  }

  def setMinDocFreq(minDocFreq: Int): this.type = {
    require(minDocFreq > 0, "最小文档频率必须大于0")
    this.minDocFreq = minDocFreq
    this
  }

  def setToTFIDF(toTFIDF: Boolean): this.type = {
    this.toTFIDF = toTFIDF
    this
  }

  def setVocabSize(vocabSize: Int): this.type = {
    require(vocabSize > 1000, "词汇表大小必须大于1000")
    this.vocabSize = vocabSize
    this
  }

  def getBlockSize: Int = this.blockSize

  def getMinDocFreq: Int = this.minDocFreq

  def getToTFIDF: Boolean = this.toTFIDF

  def getVocabSize: Int = this.vocabSize

  /**
    * 预处理运行函数，主要进行分词等数据清洗和向量化
    * @param sc sparkContext
    * @param dataInPath 数据路径
    * @param vecModelPath 向量模型路径
    * @param mode 运行模式（train/test）, 如果是train模式，vecModelPath为保存路径；如果是test模式，vecModelPath为加载路径
    * @return
    */
  def run(sc: SparkContext, dataInPath: String,vecModelPath:String,mode:String):(RDD[(Long,Vector)],CountVectorizerModel) = {
    //分词
    val preUtils = SegmentUtils.apply()
    val trainData = preUtils.getTextFile(sc,dataInPath,blockSize).zipWithIndex().map(_.swap)
    val splitedRDD = preUtils.run(trainData)

    //向量化
    val vectorizer = new VectorizerUtils().setMinDocFreq(minDocFreq).setToTFIDF(toTFIDF).setVocabSize(vocabSize)

    var resultRDD:RDD[(Long,Vector)] = null
    var cvModel:CountVectorizerModel = null

    mode match {
      case "train" =>
        val vectorize:(RDD[LabeledPoint], CountVectorizerModel, IDFModel) = vectorizer.vectorize(splitedRDD)
        val vectorizedRDD = vectorize._1
        cvModel = vectorize._2
        val idf = vectorize._3

        resultRDD=vectorizedRDD.map(line => (line.label.toLong,line.features))
        //vectorizer.save(vecModelPath,cvModel,idf)

      case "test" =>
        val laoded:(CountVectorizerModel,IDFModel) = vectorizer.load(vecModelPath)
        cvModel = laoded._1
        val idf = laoded._2

        val vectorizedRDD = vectorizer.vectorize(splitedRDD,cvModel,idf)
        resultRDD = vectorizedRDD.map(line => (line.label.toLong,line.features))

      case _ =>
        println("处理模式不对，请输入：train/test")
        sys.exit(1)
    }
    (resultRDD,cvModel)
  }
}
