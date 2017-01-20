import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * TF-IDF
  * Created by kong on 2017/1/9.
  */
object IDFDemo {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\download\\hadoop_2.6.0_64bit")
    val spark = SparkSession.builder().master("local").appName("IDFDemo").getOrCreate()
    val sentence = spark.createDataFrame(Seq(
      (0.0,"Hi I heard about Spark"),
      (0.0,"I wish Java could use case classes"),
      (1.0,"Logistic regression models are neat"))).toDF("labels","sentence")

    //切分单词，跟Array.mkString(",")一样道理
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val words = tokenizer.transform(sentence)

    //词频统计，使用hashtable实现也可以使用CountVectorizer，默认维度2^18
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurized = hashingTF.transform(words)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurized)

    val rescaled = idfModel.transform(featurized)
    rescaled.select("labels","features").show()
    spark.close()
  }
}
