package cn.sibat.nlp

import org.apache.spark.ml.feature.{CountVectorizerModel, _}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * 生成向量模型
  * Created by kong on 2017/1/3.
  */
class VectorizerUtils(private var minDocFreq: Int, private var vocabSize: Int, private var toTFIDF: Boolean) extends Serializable {
  def this() = this(minDocFreq = 1, vocabSize = 5000, toTFIDF = true)

  def setMinDocFreq(minDocFreq: Int): this.type = {
    require(minDocFreq > 0, "最小文档频率必须大于0")
    this.minDocFreq = minDocFreq
    this
  }

  def setVocabSize(vocabSize: Int): this.type = {
    require(vocabSize > 1000, "词汇表大小不小于1000")
    this.vocabSize = vocabSize
    this
  }

  def setToTFIDF(toTFIDF: Boolean): this.type = {
    this.toTFIDF = toTFIDF
    this
  }

  def getMinDocFreq: Int = this.minDocFreq

  def getVocabSize: Int = this.vocabSize

  def getToTFIDF: Boolean = this.toTFIDF

  /**
    * 生成向量模型
    *
    * @param df        DataFrame(id,tokens)
    * @param vocabSize 词汇表大小
    * @return 模型
    */
  def genCvModel(df: DataFrame, vocabSize: Int): CountVectorizerModel = {
    val cvModel = new CountVectorizer().setInputCol("tokens").setOutputCol("features").setVocabSize(vocabSize).fit(df)
    cvModel
  }

  /**
    * 把词频转化为特征LabeledPoint
    *
    * @param df      DataFrame(id,tokens)
    * @param cvModel 向量模型
    * @return
    */
  def toTFLabeledPoint(df: DataFrame, cvModel: CountVectorizerModel): RDD[LabeledPoint] = {
    val documents: DataFrame = cvModel.transform(df).select("id", "features")
    import df.sparkSession.implicits._
    documents.map {
      case Row(id: Long, features: Vector) => LabeledPoint(id, features)
    }.rdd
  }

  /**
    * 根据特征向量生成tf-idf模型
    *
    * @param df      DataFrame(id,tokens)
    * @param cvModel 向量模型
    * @return
    */
  def genIDFModel(df: DataFrame, cvModel: CountVectorizerModel): IDFModel = {
    val documents: DataFrame = cvModel.transform(df).select("id", "features")
    val idf = new IDF().setMinDocFreq(minDocFreq).setInputCol("features")
    idf.fit(documents)
  }

  /**
    * 把词频转化为特征LabeledPoint
    *
    * @param df       DataFrame(id,tokens)
    * @param idfModel TF-IDF模型
    * @return
    */
  def toTFIDFLabeledPoint(df: DataFrame, idfModel: IDFModel): RDD[LabeledPoint] = {
    val tfidf:DataFrame = idfModel.transform(df).select("id", "features")
    import df.sparkSession.implicits._
    tfidf.map {
      case Row(id:Long, features:Vector) => LabeledPoint(id, features)
    }.rdd
  }

  /**
    * 已分词的中文向量化
    *
    * @param df DataFrame(id,tokens)
    * @return
    */
  def vectorize(df: DataFrame): (RDD[LabeledPoint], CountVectorizerModel, IDFModel) = {
    val cvModel = genCvModel(df, vocabSize)

    var idfModel: IDFModel = null
    if (toTFIDF) {
      idfModel = genIDFModel(df, cvModel)
    }
    (toTFLabeledPoint(df, cvModel), cvModel, idfModel)
  }

  /**
    * 已分词的中文向量化
    *
    * @param data 已分词数据
    * @return
    */
  def vectorize(data: RDD[(Long, scala.Seq[String])]): (RDD[LabeledPoint], CountVectorizerModel, IDFModel) = {
    val sqlContext = SQLContext.getOrCreate(data.sparkContext)
    import sqlContext.implicits._

    val tokenDF = data.toDF("id", "tokens")
    vectorize(tokenDF)
  }

  /**
    * 根据cvModel与idfModel把数据转化为LabeledPoint
    *
    * @param data    输入数据
    * @param cvModel 向量模型
    * @param idfModel  idf模型
    * @return
    */
  def vectorize(data: RDD[(Long, scala.Seq[String])], cvModel: CountVectorizerModel, idfModel: IDFModel): RDD[LabeledPoint] = {
    val sqlContext = SQLContext.getOrCreate(data.sparkContext)
    import sqlContext.implicits._

    val tokenDF = data.toDF("id", "tokens")

    var tokensLP = toTFLabeledPoint(tokenDF, cvModel)
    if (toTFIDF) {
      tokensLP = toTFIDFLabeledPoint(tokenDF, idfModel)
    }
    tokensLP
  }

  /**
    * 保存cvModel向量模型和idf模型
    * @param modelPath 路径
    * @param cvModel cvModel
    * @param idfModel IDF模型
    */
  def save(modelPath: String, cvModel: CountVectorizerModel, idfModel: IDFModel): Unit = {
    idfModel.save(modelPath+"/idfModel")
    cvModel.save(modelPath+"/cvModel")
  }

  /**
    * 加载cvModel向量模型和idf模型
    *
    * @param modelPath 路径
    * @return (cvModel,idf)
    */
  def load(modelPath:String): (CountVectorizerModel,IDFModel) ={
    val idfModel = IDFModel.load(modelPath+"/idfModel")
    val cvModel = CountVectorizerModel.load(modelPath+"/cvModel")
    (cvModel,idfModel)
  }
}

