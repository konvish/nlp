import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * pca 降维
  * Created by kong on 2017/1/10.
  */
object PCADemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\download\\hadoop_2.6.0_64bit")
    val spark = SparkSession.builder().master("local").appName("PCADemo").getOrCreate()

    val data = Array(
      Vectors.sparse(5,Seq((1,1.0),(3,7.0))),
      Vectors.dense(2.0,0.0,3.0,4.0,5.0),
      Vectors.dense(4.0,0.0,0.0,6.0,7.0)
    )

    data.map(Tuple1.apply).foreach(s=> println(s._1))

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)

    val result = pca.transform(df).show(false)
  }
}
