import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SparkSession

/**
  * n元语言模型
  * Created by kong on 2017/1/10.
  */
object NGramDemo {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\download\\hadoop_2.6.0_64bit")
    val spark = SparkSession.builder().master("local").appName("CvModel").getOrCreate()

    val wordDataDF = spark.createDataFrame(Seq(
      (0,Array("Hi","I","heard","about","Spark")),
      (1,Array("I","wish","Java","could","use","case","classes")),
      (2,Array("Logistic","regression","models","are","neat"))
    )).toDF("id","words")

    val ngram = new NGram()
      .setN(2)
      .setInputCol("words")
      .setOutputCol("ngrams")
    val ngramDF = ngram.transform(wordDataDF)
    ngramDF.select("ngrams").show(false)
  }
}
