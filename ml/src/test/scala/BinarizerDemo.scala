import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession

/**
  * 二分类处理
  * Created by kong on 2017/1/10.
  */
object BinarizerDemo {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\download\\hadoop_2.6.0_64bit")
    val spark = SparkSession.builder().master("local").appName("CvModel").getOrCreate()

    val data = Array((0,0.1),(1,0.8),(2,0.2))
    val df = spark.createDataFrame(data).toDF("id","features")

    val binarizer:Binarizer = new Binarizer()
      .setInputCol("features")
      .setOutputCol("binarized_features")
      .setThreshold(0.5)

    val binarized = binarizer.transform(df)

    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
    binarized.show()
  }
}
