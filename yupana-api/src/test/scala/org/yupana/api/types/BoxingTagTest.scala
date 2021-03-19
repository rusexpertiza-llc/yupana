package org.yupana.api.types

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BoxingTagTest extends AnyFlatSpec with Matchers {

  "BoxingTag" should "provide java boxing classes for primitive types" in {
    val intBt = BoxingTag[Int]
    intBt.clazz shouldEqual classOf[java.lang.Integer]
    intBt.cast(5) shouldEqual java.lang.Integer.valueOf(5)

    val doubleBt = BoxingTag[Double]
    doubleBt.clazz shouldEqual classOf[java.lang.Double]
  }

  it should "keep the same class for references" in {
    val stringBt = BoxingTag[String]
    stringBt.clazz shouldEqual classOf[String]
    stringBt.cast("foo") shouldEqual "foo"

    val listIntBt = BoxingTag[List[Int]]
    listIntBt.clazz shouldEqual classOf[List[Int]]
  }

  it should "support arrays" in {
    val intArrayBt = BoxingTag[Array[Int]]
    intArrayBt.clazz shouldEqual classOf[Array[Int]]
  }

}
