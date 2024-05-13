package org.yupana.core.sql

import org.scalatest.{ EitherValues, Inside }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.core.TestSchema
import org.yupana.core.sql.parser.{ Constant, FieldName, Placeholder, Plus, TypedValue }

class TyperTest extends AnyFlatSpec with Matchers with EitherValues with Inside {
  "Typer" should "type fields" in {
    val typer = new Typer(TestSchema.testTable)
    val res = typer.deduce(FieldName("testField"))
    println(s"RES = $res")
  }

  it should "type constants" in {
    val typer = new Typer(TestSchema.testTable)
    val res = typer.deduce(Constant(TypedValue(25)))
    println(s"RES = $res")
  }

  it should "handle placeholders" in {
    val typer = new Typer(TestSchema.testTable)
    val res = typer.deduce(Constant(Placeholder(1)))
    println(s"RES = $res")
  }

  it should "handle simple math with all known types" in {
    val typer = new Typer(TestSchema.testTable)
    val res = typer.deduce(Plus(FieldName("testField"), Constant(TypedValue(25))))

    println(s"res= $res")
  }
}
