import cn.sibat.ml.LDAUtils
import cn.sibat.nlp.PreProcessUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/1/6.
  */
object LDATrainDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LDATrain").master("local").config("spark.local.dir","D:/temp/").getOrCreate()

    val args = Array("D:/download/CkoocNLP-master/ckooc-ml/data/news/train/","ml/models/vectorize2","ml/models/lda2")

    val dataPath = args(0)
    val vecModelPath = args(1)
    val ldaModelPath = args(2)

    val blockSize = 48
    val minDocSize = 2
    val vocabSize = 5000
    val toTFIDF = true

    val preUtils = new PreProcessUtils()
      .setBlockSize(blockSize)
      .setMinDocFreq(minDocSize)
      .setToTFIDF(toTFIDF)
      .setVocabSize(vocabSize)

    val train = preUtils.run(spark.sparkContext,dataPath,vecModelPath,"train")._1

    val k = 20
    val analysisType = "em"
    val maxIterations = 20
    val ldaUtils = new LDAUtils()
      .setK(k)
      .setAlgorithm(analysisType)
      .setMaxIterations(maxIterations)

    val ldaModel = ldaUtils.train(train)
    ldaUtils.save(ldaModelPath,ldaModel)
    spark.close()
  }
}
