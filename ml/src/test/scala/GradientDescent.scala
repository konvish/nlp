/**
  * Created by kong on 2017/1/19.
  */
class GradientDescent(alpha: Double) {
  def train(): Unit = {
    var iter = 0
    var lossChange = Double.MaxValue
    var pLossValue = Double.MaxValue
    val num = 100
    val minLoss = 0.001
    val theta = Array(1.0, 2.0)
    val data = Array(1.0, 2.0)
    val y = Array(12.0, 12.0)
    val dimension = 10
    while (iter < 1000 && lossChange > minLoss) {
      for (i <- 0 until num; if lossChange > minLoss) {
        var hi = 0.0
        for (j <- 0 until dimension) {
          hi += data(j) * theta(j)
        }
        for (k <- 0 until dimension) {
          theta(k) = theta(k) - alpha * (hi - y(i)) * data(k)
        }
        var lossValue = 0.0
        for (m <- 0 until num) {
          hi = 0.0
          for (n <- 0 until dimension) {
            hi += data(n) * theta(n)
          }
          lossValue += math.pow(hi - y(m), 2)
        }
        lossChange = math.abs(lossValue - pLossValue)
        pLossValue = lossValue
      }

      iter += 1
    }
  }
}
