package org.yupana.core.utils

import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparseTableTest extends AnyFlatSpec with Matchers with OptionValues {

  val testData = Map(
    "foo" -> Map(1 -> "foo 1", 2 -> "foo 2"),
    "bar" -> Map(1 -> "bar 1", 2 -> "bar 2", 3 -> "bar 3"),
    "baz" -> Map(2 -> "baz 2", 4 -> "baz 4")
  )

  val testTable = SparseTable(testData)

  "SparseTable" should "get values" in {
    testTable.get("foo", 1).value shouldEqual "foo 1"
    testTable.get("bar", 1).value shouldEqual "bar 1"
    testTable.get("foo", 3) shouldBe empty
    testTable.get("baz", 4).value shouldEqual "baz 4"
  }

  it should "get rows" in {
    testTable.row("foo") shouldEqual Map(1 -> "foo 1", 2 -> "foo 2")
    testTable.row("qux") shouldBe empty
  }

  it should "get columns" in {
    testTable.column(1) shouldEqual Map("foo" -> "foo 1", "bar" -> "bar 1")
    testTable.column(2) shouldEqual Map("foo" -> "foo 2", "bar" -> "bar 2", "baz" -> "baz 2")
    testTable.column(3) shouldEqual Map("bar" -> "bar 3")
    testTable.column(5) shouldBe empty
  }

  it should "transpose" in {
    val transposedData = Map(
      1 -> Map("foo" -> "foo 1", "bar" -> "bar 1"),
      2 -> Map("foo" -> "foo 2", "bar" -> "bar 2", "baz" -> "baz 2"),
      3 -> Map("bar" -> "bar 3"),
      4 -> Map("baz" -> "baz 4")
    )

    val transposedTable = SparseTable(transposedData)

    testTable.transpose shouldEqual transposedTable
  }

  it should "map row keys" in {
    val upCased = testTable.mapRowKeys(_.toUpperCase)

    upCased.get("BAR", 2).value shouldEqual "bar 2"

    upCased should not equal testTable
    upCased.mapRowKeys(_.toLowerCase) shouldEqual testTable
  }

  it should "be support map operation" in {
    val mapped = testTable.map { case (r, c, v) => (r.toUpperCase, c + 1, v.toUpperCase) }

    mapped shouldEqual SparseTable(
      "FOO" -> Map(2 -> "FOO 1", 3 -> "FOO 2"),
      "BAR" -> Map(2 -> "BAR 1", 3 -> "BAR 2", 4 -> "BAR 3"),
      "BAZ" -> Map(3 -> "BAZ 2", 5 -> "BAZ 4")
    )
  }

  it should "support flatMap operation" in {
    val mapped = testTable.flatMap {
      case (r, c, v) =>
        if (r.startsWith("b")) Some((c, r.reverse, v)) else None
    }

    mapped shouldEqual SparseTable(
      1 -> Map("rab" -> "bar 1"),
      2 -> Map("rab" -> "bar 2", "zab" -> "baz 2"),
      3 -> Map("rab" -> "bar 3"),
      4 -> Map("zab" -> "baz 4")
    )
  }
}
