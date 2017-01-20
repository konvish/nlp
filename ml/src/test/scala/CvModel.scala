import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

/**
  * 词频统计
  * Created by kong on 2017/1/10.
  */
object CvModel {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\download\\hadoop_2.6.0_64bit")
    val spark = SparkSession.builder().master("local").appName("CvModel").getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0,Array("a","b","c")),
      (1,Array("a","b","b","c","a"))
    )).toDF("id","words")

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    val cvm = new CountVectorizerModel(Array("a","b","c")).setInputCol("words").setOutputCol("features")
    cvModel.transform(df).show()
  }
}
