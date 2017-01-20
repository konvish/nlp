import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession

/**
  * 过滤一些停用词适用英语
  * Created by kong on 2017/1/10.
  */
object StopWordsRemoverDemo {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\download\\hadoop_2.6.0_64bit")
    val spark = SparkSession.builder().master("local").appName("StopWordsRemoverDemo").getOrCreate()

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show(false)
  }
}
