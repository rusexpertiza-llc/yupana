package org.yupana.core.settings

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Period

class SettingsTest extends AnyFlatSpec with Matchers {

  "Settings" should "parse comma separated string values like \"a, b, c\\, d\" to Set of String" in {
    val res = settings[Set[String]]("escaped_comma")
    res should contain theSameElementsAs List("a", "b", "c, d")
  }

  it should "parse separated by LF string values like \"a\\nb\\nc, d\" to Set of String" in {
    val res = settings("multiline_LF", Set[String]())
    res should contain theSameElementsAs List("a", "b", "c", "d")
  }

  it should "parse separated by CR string values like \"a\\rb\\rc, d\" to Set of String" in {
    val res = settings("multiline_CR", Set[String]())
    res should contain theSameElementsAs List("a", "b", "c", "d")
  }

  it should "parse separated by CR+LF string values like \"a\\r\\nb\\r\\nc, d\" to Set of String" in {
    val res = settings("multiline_CR+LF", Set[String]())
    res should contain theSameElementsAs List("a", "b", "c", "d")
  }

  it should "parse not existing property to Option" in {
    val res = settings.opt[String]("missing setting")
    res shouldBe empty
  }

  it should "parse existing property to Option" in {
    val res = settings.opt[String]("string")
    res should not be empty
    res.get should be("some string")
  }

  it should "parse existing property with empty value to Option when allowEmpty is true" in {
    val res = settings.opt[String]("empty_string", true)
    res should not be empty
    res.get should be("")
  }

  it should "parse existing empty value to Option when allowEmpty is false" in {
    val res = settings.opt[String]("empty_string")
    res shouldBe empty
  }

  it should "throw an SettingsException while parsing an existing value to incompatible type when allowEmpty is true" in {
    an[SettingsException] should be thrownBy settings.opt[Period]("empty_string", true)
  }

  it should "throw an SettingsException when apply is called with not existing property name" in {
    an[SettingsException] should be thrownBy settings[String]("missing setting")
  }

  class DummySettings(pp: Map[String, String]) extends Settings {
    override def getByKey(k: String): Option[String] = pp.get(k)
  }

  private val propertyMap: Map[String, String] = Map(
    "multiline_LF" -> "a\nb\nc, d",
    "multiline_CR" -> "a\rb\rc, d",
    "multiline_CR+LF" -> "a\r\nb\r\nc, d",
    "escaped_comma" -> "a, b, c\\, d",
    "string" -> "some string",
    "empty_string" -> ""
  )

  private val settings = new DummySettings(propertyMap)

}
