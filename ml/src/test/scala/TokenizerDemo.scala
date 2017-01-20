import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * 单词切分
  * Created by kong on 2017/1/10.
  */
object TokenizerDemo {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local").appName("CvModel").getOrCreate()

    val sentenceDF = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("id", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W")

    val countToken = udf { (words: Seq[String]) => words.length }

    val tokenized = tokenizer.transform(sentenceDF)

    tokenized.select("sentence", "words").withColumn("tokens", countToken(col("words"))).show(false)

    val regexTokenized = regexTokenizer.transform(sentenceDF)
    regexTokenized.select("sentence","words").withColumn("tokens",countToken(col("words"))).show(false)
  }
}
