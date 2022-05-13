package org.yupana.khipu

import jdk.incubator.foreign.MemorySegment
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class StorageFormatTest extends AnyFlatSpec with Matchers {

  "A StorageFormat" should "startsWith" in {

    val v =
      MemorySegment.ofArray("12345678123456781234567812345678123456781234567812345678".getBytes(StandardCharsets.UTF_8))
    val p = MemorySegment.ofArray("1234567812345678".getBytes(StandardCharsets.UTF_8))

    StorageFormat.startsWith(v, p, 54) shouldBe true
    StorageFormat.startsWith(v, p, 54) shouldBe true

  }

  it should "startsWiths fast" in {

    def run(): Unit = {

      val b =
        MemorySegment.ofArray(
          "12345678123456781234567812345678123456781234567812345678".getBytes(StandardCharsets.UTF_8)
        )
      val p = MemorySegment.ofArray("1234567812345678".getBytes(StandardCharsets.UTF_8))

      val N = 150000000
      val r = time() {
        var fl = false
        var i = 0
        while (i < N) {
          fl = StorageFormat.startsWith(b, p, 54)
          i += 1
        }
        fl
      }

      r shouldBe true

    }

    (1 to 100) foreach { _ => run() }

  }

  def time[R](msg: String = "Elapsed time: ")(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println(msg + (t1 - t0) / 1e9 + "s")
    result
  }

}
