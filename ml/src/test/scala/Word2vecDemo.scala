import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by kong on 2017/1/9.
  */
object Word2vecDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\download\\hadoop_2.6.0_64bit")
    val spark = SparkSession.builder().master("local").appName("IDFDemo").config("spark.local.dir", "D:/temp/").getOrCreate()
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    import spark.implicits._
    val data = spark.sparkContext.textFile("D:/workspace/pycharm/test/com/kong/test/nlp/wiki.split.zh.text").map(s => s.split(" ").toSeq).toDF("text")

    val word2vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text:[${text.mkString(",")}] => \nVector: $features\n")
    }
    spark.close()
  }
}
