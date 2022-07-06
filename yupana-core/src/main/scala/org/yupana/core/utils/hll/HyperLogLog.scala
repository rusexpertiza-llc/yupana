package org.yupana.core.utils.hll

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

case class HyperLogLog(
    numBitsForRegisterIndex: Int,
    numBitsForRegisterValue: Int
) {

  val registerSize: Int = math.pow(2, numBitsForRegisterIndex).toInt

  val register: ArrayBuffer[Int] = ArrayBuffer.fill(registerSize)(0)

  val alpha: Double = registerSize match {
    case 16 => 0.673
    case 32 => 0.697
    case 64 => 0.709
    case _  => 0.7213 / (1 + 1.079 / registerSize)
  }

  def addValue(value: Int): Unit = {
    val hash = MurmurHash3.stringHash(value.toString, 11).toBinaryString
    val registerIndex = Integer.parseInt(hash.takeRight(numBitsForRegisterIndex), 2)
    val zeroRunLength =
      hash.dropRight(numBitsForRegisterIndex).takeRight(numBitsForRegisterValue).reverse.takeWhile(_ != '1').length
    val registerValue = zeroRunLength + 1
    register(registerIndex) = if (register(registerIndex) < registerValue) registerValue else register(registerIndex)
  }

  def getCount: Int = {
    val m = 1 / register.toSeq.foldLeft(0: Double)((a, b) => a + math.pow(2, -1 * b))
    val countEstimate = alpha * math.pow(registerSize, 2) * m
    correctEstimate(countEstimate).toInt
  }

  def correctEstimate(estimate: Double): Double = {
    val correctedEstimate = estimate match {
      case 1 if estimate < 5 / 2 * registerSize => {
        val zeroRegisterCount = register.count(_ == 0)
        if (zeroRegisterCount > 0) registerSize * math.log(registerSize / zeroRegisterCount) else estimate
      }
      case 1 if estimate > (1 / 30 * math.pow(2, 32)) => {
        -1 * math.pow(2, 32) * math.log(1 - estimate / math.pow(2, 32))
      }
      case _ => estimate
    }
    correctedEstimate
  }
}
