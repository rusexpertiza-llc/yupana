package org.yupana.khipu

import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.api.schema.{ Metric, RawDimension, Table }
import org.yupana.core.QueryContext
import org.yupana.core.model.InternalRowBuilder
import org.yupana.khipu.storage.StorageFormat
import org.yupana.api.query.syntax.All._
import org.yupana.core.jit.JIT
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.readerwriter.{ ByteBufferEvalReaderWriter, MemoryBuffer }

import java.lang.foreign.{ Arena, MemorySegment, ValueLayout }
import java.nio.{ ByteBuffer, ByteOrder }
import java.nio.charset.StandardCharsets
import java.time.{ LocalDateTime, ZoneOffset }

@Ignore
class StorageFormatTest extends AnyFlatSpec with Matchers {

  "A StorageFormat" should "startsWith" in {

    val v =
      StorageFormat.fromBytes(
        "12345678123456781234567812345678123456781234567812345678".getBytes(StandardCharsets.UTF_8)
      )
    val p = StorageFormat.fromBytes("1234567812345678".getBytes(StandardCharsets.UTF_8))

    StorageFormat.startsWith(v, p, 54) shouldBe true
    StorageFormat.startsWith(v, p, 54) shouldBe true

  }

  it should "test read dimension values" in {

//    val CAT = RawDimension[Long]("cat")
//    val BRAND = RawDimension[Int]("brand")
//    val DEVICE = RawDimension[Int]("device")
//    val OPERATION = RawDimension[Byte]("operation")
//    val POS = RawDimension[Byte]("pos")

    val K = 200
    val M = 10000000
    val N = 10

    val seg = Arena.ofConfined().allocate(M * 24, 8)
    var s = 0L
    var i = 0
    while (i < K) {
      measure("ByteBuffer segment", M.toLong * N * 24) {
        var j = 0
        while (j < N) {
          var m = 0
          while (m < M) {
            val ms = seg.asSlice(m * 24, 24)
            val memBuf = MemoryBuffer.ofMemorySegment(ms)

//            val buf = ms.asByteBuffer()
//            val c = CAT.rStorable.read(buf)
//            val b = BRAND.rStorable.read(buf)
//            val d = DEVICE.rStorable.read(buf)
//            val o = OPERATION.rStorable.read(buf)
//            val p = POS.rStorable.read(buf)
//            val c = buf.getLong()
//            val b = buf.getInt()
//            val d = buf.getInt()
//            val o = buf.get()
//            val p = buf.get()
//            s = s + c + b + d + o + p
//            s += SFTest.testBuf(s, buf)

            s += SFTest.testMemBuf(s, memBuf)

            m += 1
          }
          j += 1
        }
      }
      i += 1
    }
    println(s)
  }

  it should "test read row values to InternalRow" in {
    implicit val rw = ByteBufferEvalReaderWriter

    val CAT = RawDimension[Long]("cat")
    val BRAND = RawDimension[Int]("brand")
    val DEVICE = RawDimension[Int]("device")
    val OPERATION = RawDimension[Byte]("operation")
    val POS = RawDimension[Byte]("pos")
    val QUANTITY = Metric[Double]("quantity", 1)
    val SUM = Metric[Long]("sum", 2)

    val epochTime: Long = LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli

    val testTable = new Table(
      1,
      "test",
      rowTimeSpan = 86400000L * 30L,
      dimensionSeq = Seq(CAT, BRAND, DEVICE, OPERATION, POS),
      metrics = Seq(QUANTITY, SUM),
      externalLinks = Seq.empty,
      epochTime = epochTime
    )

    val exprs = Seq(
      dimension(CAT) as "cat",
      dimension(BRAND) as "brand",
      dimension(DEVICE) as "device",
      dimension(OPERATION) as "operation",
      dimension(POS) as "pos"
    )

    val query = Query(
      testTable,
      const(Time(10)),
      const(Time(20)),
      exprs,
      None,
      Seq.empty
    )

    val queryContext = new QueryContext(query, None, JIT, NoMetricCollector)

    val rowBuilder = new InternalRowBuilder(queryContext)

    val K = 20
    val M = 10000000
    val N = 10

    val seg = Arena.ofConfined().allocate(M * 64, 8)
    (0 until M).foldLeft(0) {
      case (offset, i) =>
        StorageFormat.setLongUnaligned(i.toLong, seg, offset + 4)
        StorageFormat.setIntUnaligned(i, seg, offset + 4 + 8)

        StorageFormat.setByte(1.toByte, seg, offset + 4 + 8 + 4 + 4 + 1 + 1)
        seg.set(
          ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN),
          offset + 4 + 8 + 4 + 4 + 1 + 1 + 1,
          i.toDouble
        )

        StorageFormat.setByte(2.toByte, seg, offset + 4 + 8 + 4 + 4 + 1 + 1 + 1 + 8)
        val b2 = Array.ofDim[Byte](9)
        val bb = ByteBuffer.wrap(b2)
        rw.writeVLong(bb, i + 10)
        StorageFormat.setBytes(b2, 0, seg, offset + 4 + 8 + 4 + 4 + 1 + 1 + 1 + 8 + 1, bb.position())
        val size = 4 + 8 + 4 + 4 + 1 + 1 + 1 + 8 + 1 + bb.position()
//        val size = 40
        StorageFormat.setInt(size, seg, offset)
        offset + StorageFormat.alignLong(size)
    }

    var s = 0L
    var i = 0
    while (i < K) {
      measure("ByteBuffer segment", M.toLong * N * 40) {
        var j = 0
        while (j < N) {
          var m = 0
          var offset = 0
          while (m < M) {
            val ms = seg.asSlice(offset, 22)
            val buf = ms.asByteBuffer()

            val size = buf.getInt

            val c = CAT.rStorable.read(buf)
            val b = BRAND.rStorable.read(buf)
            val d = DEVICE.rStorable.read(buf)
            val o = OPERATION.rStorable.read(buf)
            val p = POS.rStorable.read(buf)

//            val size = buf.getInt
////            if (size != 40) println(size)
//            val c = buf.getLong()
//            val b = buf.getInt()
//            val d = buf.getInt()
//            val o = buf.get()
//            val p = buf.get()

            rowBuilder.set((Table.DIM_TAG_OFFSET + 0).toByte, c)
            rowBuilder.set((Table.DIM_TAG_OFFSET + 1).toByte, b)
            rowBuilder.set((Table.DIM_TAG_OFFSET + 2).toByte, d)
            rowBuilder.set((Table.DIM_TAG_OFFSET + 3).toByte, o)
            rowBuilder.set((Table.DIM_TAG_OFFSET + 4).toByte, p)

            val msv = seg.asSlice(offset + 22, size - 22)
            val bb = msv.asByteBuffer()

//            while (bb.hasRemaining) {
//              val tag = bb.get()
//              tag match {
//                case 1 =>
//                  rowBuilder.set(tag, bb.getDouble())
//                case 2 =>
//                  rowBuilder.set(tag, Storable.readVLong(bb))
//                case _ =>
//              }
//            }

            while (bb.hasRemaining) {
              val tag = bb.get()

              testTable.fieldForTag(tag) match {
                case Some(Left(metric)) =>
                  val v = metric.dataType.storable.read(bb)
                  rowBuilder.set(tag, v)(metric.dataType.internalStorable)
//                    s += v.hashCode()
                case _ =>
                  throw new IllegalStateException(
                    s"Unknown tag: $tag [${testTable.fieldForTag(tag)}] , in table: ${testTable.name}"
                  )
              }

            }
            rowBuilder.buildAndReset()
//            val row = rowBuilder.buildAndReset()
//            val t = row.get[Int](1)
            s += c + b + d + o + p // + t
            m += 1
            offset += StorageFormat.alignLong(size)
          }
          j += 1
        }
      }
      i += 1
    }
    rowBuilder.buildAndReset()
    println(s)
  }

  it should "test read dimension values in to InternalRow by calculator2" in {

    val CAT = RawDimension[Long]("cat")
    val BRAND = RawDimension[Int]("brand")
    val DEVICE = RawDimension[Int]("device")
    val OPERATION = RawDimension[Byte]("operation")
    val POS = RawDimension[Byte]("pos")
    val QUANTITY = Metric[Double]("quantity", 1)
    val SUM = Metric[Long]("sum", 2)

    val epochTime: Long = LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli

    val testTable = new Table(
      1,
      "test",
      rowTimeSpan = 86400000L * 30L,
      dimensionSeq = Seq(CAT, BRAND, DEVICE, OPERATION, POS),
      metrics = Seq(QUANTITY, SUM),
      externalLinks = Seq.empty,
      epochTime = epochTime
    )

    val exprs = Seq(
      dimension(CAT) as "cat",
      dimension(BRAND) as "brand",
      dimension(DEVICE) as "device",
      dimension(OPERATION) as "operation",
      dimension(POS) as "pos",
      metric(QUANTITY) as "qnt",
      metric(SUM) as "sum"
    )

    val query = Query(
      testTable,
      const(Time(10)),
      const(Time(20)),
      exprs,
      None,
      Seq.empty
    )

    val queryContext = new QueryContext(query, None, JIT, NoMetricCollector)

    val calc = queryContext.calculator

    val rowBuilder = new InternalRowBuilder(queryContext)

    val K = 10
    val M = 10000000
    val N = 10

    val seg = Arena.ofConfined().allocate(M * 64, 8)

    {
      implicit val rw = ByteBufferEvalReaderWriter

      (0 until M).foldLeft(0) {
        case (offset, i) =>
          StorageFormat.setLongUnaligned(i.toLong, seg, offset + 4) // base time
          StorageFormat.setLongUnaligned(i.toLong, seg, offset + 4 + 8) // cat
          StorageFormat.setIntUnaligned(i, seg, offset + 4 + 8 + 8) // brand
          StorageFormat.setIntUnaligned(i, seg, offset + 4 + 8 + 8 + 4) // device
          StorageFormat.setByte(i.toByte, seg, offset + 4 + 8 + 8 + 4 + 4) // operation
          StorageFormat.setByte(i.toByte, seg, offset + 4 + 8 + 8 + 4 + 4 + 1) // pos
          StorageFormat.setLongUnaligned(i.toLong, seg, offset + 4 + 8 + 8 + 4 + 4 + 1 + 1) // rest time

          val keySize = 4 + 8 + 8 + 4 + 4 + 1 + 1 + 8
          StorageFormat.setByte(1.toByte, seg, offset + keySize)
          seg.set(
            ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN),
            offset + keySize + 1,
            i.toDouble
          )

          StorageFormat.setByte(2.toByte, seg, offset + keySize + 1 + 8)
          val b2 = Array.ofDim[Byte](9)
          val bb = ByteBuffer.wrap(b2)
          rw.writeVLong(bb, 1) // i + 10)
          StorageFormat.setBytes(b2, 0, seg, offset + keySize + 1 + 8 + 1, bb.position())
          val size = keySize + 1 + 8 + 1 + bb.position()

          StorageFormat.setInt(size, seg, offset)
          offset + StorageFormat.alignLong(size)
      }
    }

    var s = 0L
    var i = 0
    while (i < K) {
      measure("ByteBuffer segment", M.toLong * N * 47) {
        var j = 0
        while (j < N) {
          var m = 0
          var offset = 0
          while (m < M) {
            val size = StorageFormat.getInt(seg, offset)
            val buf = MemoryBuffer.ofMemorySegment(seg.asSlice(offset + 4, size - 4))

//            val row = calc.evaluateReadRow2(buf, rowBuilder)
//            val t = row.get[Int](1, rowBuilder)
//            s = t + s
            calc.evaluateReadRow(buf, rowBuilder)
            s += 1
            m += 1
            offset += StorageFormat.alignLong(size)
          }
          j += 1
        }
      }
      i += 1
    }
    println(s)
  }

  it should "test performance of different types of memory access" in {
    val K = 20
    val M = 10000000
    val N = 5

    var s = 0L

    val arr = Array.ofDim[Long](M + N)
    (0 until M + N).foreach { i => arr(i) = i }

    var i = 0
    while (i < K) {
      measure("array time", M.toLong * N * 8) {
        var m = 0
        while (m < M) {
          var j = 0
          while (j < N) {
            s += SFTest.testArr(arr, m, j)
            j += 1
          }
          m += 1
        }
      }
      i += 1
    }
    println(s)

    val seg = Arena.ofConfined().allocate((M + N) * 8, 8)

    (0 until M + N).foreach { i => StorageFormat.setLong(i.toLong, seg, i * 8) }

    i = 0
    s = 0
    while (i < K) {
      measure("native segment", M.toLong * N * 8) {
        var m = 0
        while (m < M) {
          var j = 0
          while (j < N) {
            s += StorageFormat.getLong(seg, m * 8 + j * 8)
//            s += seg.getAtIndex(SFTest.layout, m  + j )
            j += 1
          }
          m += 1
        }
      }
      i += 1
    }
    println(s)

    i = 0
    s = 0
    while (i < K) {
      measure("MyBuffer segment", M.toLong * N * 8) {
        var m = 0
        while (m < M) {
          var j = 0
          val ms = seg.asSlice(m * 8, N * 8)
          val buf = new MyBuffer(ms)
          while (j < N) {
            s += buf.getLong
            j += 1
          }
          m += 1
        }
      }
      i += 1
    }

    println(s)

    i = 0
    s = 0
    while (i < K) {
      measure("ByteBuffer segment", M.toLong * N * 8) {
        var m = 0
        while (m < M) {
          val ms = seg.asSlice(m * 8, N * 8)
          val buf = ms.asByteBuffer()
          var j = 0
          while (j < N) {
            s += buf.getLong
            j += 1
          }
          m += 1
        }
      }
      i += 1
    }

    println(s)

  }

  def measure[R](msg: String = "Elapsed time: ", size: Long)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val sec = (t1 - t0).toDouble / 1e9
    val gbs = size.toDouble / sec / 1024 / 1024 / 1024
    println(s"$msg time: ${sec * 1000} ms, throughput $gbs GB/S")
    result
  }

}

final class MyBuffer(private val segment: MemorySegment) {
  private var offset = 0

  def getLong: Long = {
    val o = offset
    offset += 8
    StorageFormat.getLong(segment, o)
  }
}

object SFTest {
  final val layout = ValueLayout.JAVA_LONG.withOrder(ByteOrder.BIG_ENDIAN)

  def testBuf(s: Long, buf: ByteBuffer): Long = {
    val c = buf.getLong(0)
//    val b = buf.getInt()
//    val d = buf.getInt()
//    val o = buf.get()
//    val p = buf.get()
//    c + b + d + o + p
    c
  }

  def testBuf(s: Long, ms: MemorySegment): Long = {
    val c = ms.get(ValueLayout.JAVA_LONG, 0)
    //    val b = buf.getInt()
    //    val d = buf.getInt()
    //    val o = buf.get()
    //    val p = buf.get()
    //    c + b + d + o + p
    c
  }

  def testMemBuf(s: Long, buf: MemoryBuffer): Long = {
//    val c = buf.getLong(0)
//    val b = buf.getInt(8)
//    val d = buf.getInt(12)
//    val o = buf.get(16)
//    val p = buf.get(17)
    val c = buf.getLong()
    val b = buf.getInt()
    val d = buf.getInt()
    val o = buf.get()
    val p = buf.get()
    c + b + d + o + p
  }

  def testArr(arr: Array[Long], m: Int, j: Int): Long = {
    arr(m + j)
  }
}
