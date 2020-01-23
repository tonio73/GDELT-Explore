package fr.telecom

// Utility functions to perform data mangling when create DataSets
object DataMangling {

  def toDouble(s: String): Double = {
    try {
      if (s.isEmpty) 0 else s.toDouble
    }
    catch {
      case e: Exception => {
        0
      }
    }
  }

  def toInt(s: String): Int = {
    try {
      if (s.isEmpty) 0 else s.toInt
    }
    catch {
      case e: Exception => {
        0
      }
    }
  }

  def toLong(s: String): Long = {
    try {
      if (s.isEmpty) 0 else s.toLong
    }
    catch {
      case e: Exception => {
        0
      }
    }
  }

  def toBigInt(s: String): BigInt = {
    try {
      if (s.isEmpty) 0 else BigInt(s)
    }
    catch {
      case e: Exception => {
        0
      }
    }
  }
}
