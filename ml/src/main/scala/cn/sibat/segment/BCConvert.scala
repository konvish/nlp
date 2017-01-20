package cn.sibat.segment

/**
  * 字符串全角半角之间转换
  * Created by kong on 2016/12/29.
  */
object BCConvert {
  //ASCII表中可见字符从!开始，偏移位值为33(Decimal)
  val DBC_CHAR_START:Char = 33
  //ASCII表中可见字符到~结束，偏移位值为126(Decimal)
  val DBC_CHAR_END:Char = 126
  //全角对应于ASCII表的可见字符从！开始，偏移值为65281
  val SBC_CHAR_START:Char = 65281
  //全角对应于ASCII表的可见字符到～结束，偏移值为65374
  val SBC_CHAR_END:Char = 65374
  //ASCII表中除空格外的可见字符与对应的全角字符的相对偏移
  val CONVERT_STEP:Int = 65248
  //全角空格的值，它没有遵从与ASCII的相对偏移，必须单独处理
  val SBC_SPACE:Char = 12288
  //半角空格的值，在ASCII中为32(Decimal)
  val DBC_SPACE:Char = ' '

  /**
    * 半角字符->全角字符转换 只处理空格，!到˜之间的字符，忽略其他
    * @param src 字符串
    * @return
    */
  def b2q(src: String): String = {
    if (src == null)
      null
    val sb = new StringBuilder(src.length)
    val ca = src.toCharArray
    for (aCa <- ca) {
      if (aCa == DBC_SPACE) {
        sb.append(SBC_SPACE)
      }
      else if ((aCa >= DBC_CHAR_START) && (aCa <= DBC_CHAR_END)) {
        sb.append((aCa + CONVERT_STEP).toChar)
      }
      else {
        sb.append(aCa)
      }
    }
    sb.toString()
  }

  /**
    * 全角字符->半角字符转换 只处理空格，!到˜之间的字符，忽略其他
    * @param src 字符串
    * @return
    */
  def q2b(src: String): String = {
    if (src == null)
      null
    val sb = new StringBuilder(src.length)
    val ca = src.toCharArray
    for (aCa <- ca) {
      if (aCa == SBC_SPACE) {
        sb.append(DBC_SPACE)
      }
      else if ((aCa >= SBC_CHAR_START) && (aCa <= SBC_CHAR_END)) {
        sb.append((aCa - CONVERT_STEP).toChar)
      }
      else {
        sb.append(aCa)
      }
    }
    sb.toString()
  }

}
