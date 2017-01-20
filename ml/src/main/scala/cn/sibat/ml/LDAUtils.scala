package cn.sibat.ml

import java.io.File

import org.apache.spark.ml.clustering.{DistributedLDAModel, LDA, LDAModel, LocalLDAModel}
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.clustering.{EMLDAOptimizer, LDAOptimizer, OnlineLDAOptimizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * LDA算法工具类
  * Created by kong on 2017/1/3.
  */
class LDAUtils(private var k: Int,
               private var maxIterations: Int,
               private var algorithm: String,
               private var alpha: Double,
               private var beta: Double,
               private var checkpointInterval: Int,
               private var checkpointDir: String) extends Serializable {
  def this() = this(10, 20, "em", 1.05, 1.05, 10, "")

  def setK(k: Int): this.type = {
    require(k > 0, "主题个数必须大于0")
    this.k = k
    this
  }

  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations > 0, "迭代次数必须大于0")
    this.maxIterations = maxIterations
    this
  }

  def setAlgorithm(algorithm: String): this.type = {
    require(algorithm.equalsIgnoreCase("em") || algorithm.equalsIgnoreCase("online"), "参数估计只支持：em/online")
    this.algorithm = algorithm
    this
  }

  def setBeta(beta: Double): this.type = {
    this.beta = beta
    this
  }

  def setAlpha(alpha: Double): this.type = {
    this.alpha = alpha
    this
  }

  def setCheckpointInterval(checkpointInterval: Int): this.type = {
    require(checkpointInterval > 0, "检查点的间隔必须大于0")
    this.checkpointInterval = checkpointInterval
    this
  }

  def setCheckpointDir(checkpointDir: String): this.type = {
    this.checkpointDir = checkpointDir
    this
  }

  def getK: Int = k

  def getMaxIterations: Int = maxIterations

  def getAlgorithm: String = algorithm

  def getAlpha: Double = alpha

  def getBeta: Double = beta

  def getCheckpointInterval: Int = checkpointInterval

  def getCheckpointDir: String = checkpointDir

  /**
    * 选择算法(mllib库才需要)
    *
    * @param algorithm  算法名(em与online)
    * @param corpusSize 语料库大小
    * @return
    */
  private def selectOptimizer(algorithm: String, corpusSize: Long): LDAOptimizer = {
    val optimizer = algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / corpusSize)
      case _ => throw new IllegalArgumentException(s"只支持：em与online算法，输入的是：$algorithm.")
    }
    optimizer
  }

  /**
    * 模型训练
    * @param data 数据
    * @return
    */
  def train(data: RDD[(Long, Vector)]): LDAModel = {
    val sc = data.sparkContext
    val sqlContext = SQLContext.getOrCreate(data.sparkContext)
    import sqlContext.implicits._

    val df = data.toDF("id","features")
    df.show(5)

    if (checkpointDir.nonEmpty)
      sc.setCheckpointDir(checkpointDir)
    //mllib库才需要
    //    val actualCorpusSize = data.map(_._2.numActives).sum().toLong
    //    val optimizer = selectOptimizer(algorithm,actualCorpusSize)

    val lda = new LDA()
      .setK(k)
      .setOptimizer(algorithm)
      .setMaxIter(maxIterations)
      .setDocConcentration(alpha)
      .setTopicConcentration(beta)
      .setCheckpointInterval(checkpointInterval)

    //训练LDA模型
    val ldaModel = lda.fit(df)
    //trainInfo(data, ldaModel,df)

    ldaModel
  }

  /**
    * 打印模型训练相关信息
    * @param data rdd
    * @param ldaModel LDAModel
    * @param df dataFrame
    */
  def trainInfo(data: RDD[(Long, Vector)], ldaModel: LDAModel,df:DataFrame) = {
    println("完成LDA模型训练！")
    val actualCorpusSize = data.map(_._2.numActives).sum().toLong

    ldaModel match {
      case distLDAModel:DistributedLDAModel =>
        val avgLogLikelihood = distLDAModel.logLikelihood(df) / actualCorpusSize.toDouble
        val logPerplexity = distLDAModel.logPrior
        println(s"\t 训练数据平均对数似然度：$avgLogLikelihood")
        println(s"\t 训练数据对数困惑度：$logPerplexity")
        println()
      case localLDAModel: LocalLDAModel =>
        val avgLogLikelihood = localLDAModel.logLikelihood(df) / actualCorpusSize.toDouble
        val logPerplexity = localLDAModel.logPerplexity(df)
        println(s"\t 训练数据平均对数似然度：$avgLogLikelihood")
        println(s"\t 训练数据对数困惑度：$logPerplexity")
        println()
      case _ =>
    }
  }

  /**
    * 更新模型（使用已有模型的alpha和beta进行训练）
    *
    * @param df 输入数据
    * @return LDAModel
    */
  def update(df: DataFrame, ldaModel: LDAModel): LDAModel = {
    val sc = df.sparkSession.sparkContext

    if (checkpointDir.nonEmpty) {
      sc.setCheckpointDir(checkpointDir)
    }

    algorithm = ldaModel match {
      case distLDAModel: DistributedLDAModel => "em"
      case localLDAModel: LocalLDAModel => "online"
    }

    //获取模型的alpha、beta
    val alphaVector = ldaModel.getDocConcentration
    beta = ldaModel.getTopicConcentration
    k = ldaModel.getK

    val lda = new LDA()
      .setK(k)
      .setOptimizer(algorithm)
      .setMaxIter(maxIterations)
      .setDocConcentration(alphaVector)
      .setTopicConcentration(beta)
      .setCheckpointInterval(checkpointInterval)

    val newModel = lda.fit(df)

    newModel
  }

  /**
    * LDA新文档预测
    * @param data 输入数据
    * @param ldaModel LDA模型
    * @param cvModel 向量模型
    * @param sorted 是否排序(默认false)
    * @return
    */
  def predict(data:RDD[(Long,Vector)],ldaModel:LDAModel,cvModel:CountVectorizerModel,sorted:Boolean = false):(DataFrame,DataFrame) = {
    val sqlContext = SQLContext.getOrCreate(data.sparkContext)
    import sqlContext.implicits._
    val df = data.toDF()
    val vocabArray = cvModel.vocabulary

    var docTopics: DataFrame = null
    if (sorted){
      docTopics = getSortedDocTopics(df,ldaModel,sorted)
    }else
      docTopics = getDocTopics(ldaModel,df)

    val topicWords:DataFrame = getTopicWords(ldaModel)
    (docTopics,topicWords)
  }

  /**
    * 主题描述，包括主题下每个词以及词的权重
    * @param ldaModel LDAModel
    * @return
    */
  def getTopicWords(ldaModel:LDAModel):DataFrame={
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)
    topicIndices
  }

  /**
    * 文档-主题分布结果
    *
    * @param ldaModel LDAModel
    * @param corpus   文档
    * @return “文档-主题分布”：(docID, topicDistributions)
    */
  def getDocTopics(ldaModel: LDAModel, corpus: DataFrame): DataFrame = {
    var topicDistributions: DataFrame = null
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        topicDistributions = distLDAModel.transform(corpus)
      case localLDAModel: LocalLDAModel =>
        topicDistributions = localLDAModel.transform(corpus)
      case _ =>
    }

    topicDistributions
  }

  /**
    * 排序后的文档-主题分布结果
    *
    * @param corpus   文档
    * @param ldaModel LDAModel
    * @param desc     是否降序
    * @return 排序后的“文档-主题分布”：(docID, sortedDist)
    */
  def getSortedDocTopics(corpus: DataFrame, ldaModel: LDAModel, desc: Boolean = true): DataFrame = {

    var topicDistributions: DataFrame = null
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        topicDistributions = distLDAModel.transform(corpus)
      case localLDAModel: LocalLDAModel =>
        topicDistributions = localLDAModel.transform(corpus)
      case _ =>
    }

    topicDistributions
  }


  /**
    * 保存模型和tokens
    *
    * @param modelPath 模型保存路径
    * @param ldaModel  LDAModel
    */
  def save(modelPath: String, ldaModel: LDAModel): Unit = {
    ldaModel match {
      case distModel: DistributedLDAModel =>
        distModel.toLocal.save(modelPath + File.separator + "model")
      case localModel: LocalLDAModel =>
        localModel.save(modelPath + File.separator + "model")
      case _ =>
        println("保存模型出错！")
    }
  }


  /**
    * 加载模型和tokens
    *
    * @param modelPath 模型路径
    * @return (LDAModel, tokens)
    */
  def load(modelPath: String): LDAModel = {
    val ldaModel = LocalLDAModel.load(modelPath + File.separator + "model")
    ldaModel
  }
}
