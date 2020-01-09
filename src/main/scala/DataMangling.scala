object DataMangling {
  def toDouble(s : String): Double = if (s.isEmpty) 0 else s.toDouble
  def toInt(s : String): Int = if (s.isEmpty) 0  else s.toInt
  def toBigInt(s : String): BigInt = if (s.isEmpty) BigInt(0) else BigInt(s)
}
