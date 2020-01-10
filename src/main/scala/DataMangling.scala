package fr.telecom

// Utility functions to perform data mangling when create DataSets
object DataMangling {
  def toDouble(s : String): Double = if (s.isEmpty) 0 else s.toDouble
  def toInt(s : String): Int = if (s.isEmpty) 0  else s.toInt
  def toLong(s : String): Long = if (s.isEmpty) 0  else s.toLong
  def toBigInt(s : String): BigInt = if (s.isEmpty) BigInt(0) else BigInt(s)
}
