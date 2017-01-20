import org.apache.spark.util.random.XORShiftRandom

/**
  * Created by kong on 2017/1/9.
  */
object Test {
  def main(args: Array[String]) {
    val vocabSize = 16
    val vectorSize = 1
    Array.fill[Float](vocabSize * vectorSize)((1.0f - 0.5f) / vectorSize)
  }
}
