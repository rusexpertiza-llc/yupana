package org.yupana.core.model

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }

import org.yupana.api.Time
import org.yupana.api.query.{ Query, TimeExpr }
import org.yupana.core.{ ExpressionCalculatorFactory, QueryContext, TestDims, TestSchema, TestTableFields }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.{ LocalDateTime, ZoneOffset }

class KeyDataTest extends AnyFlatSpec with Matchers {

  import org.yupana.api.query.syntax.All._

  "KeyData" should "preserve hash after serialization" in {
    val qtime = LocalDateTime.of(2019, 10, 12, 13, 47).atOffset(ZoneOffset.UTC)

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        sum(metric(TestTableFields.TEST_FIELD)) as "testField",
        truncDay(time) as "time",
        dimension(TestDims.DIM_A) as "TAG_A"
      ),
      None,
      Seq(dimension(TestDims.DIM_A))
    )
    val context = new QueryContext(query, None, ExpressionCalculatorFactory)

    val builder = new InternalRowBuilder(context)

    val original = new KeyData(
      context,
      builder
        .set(metric(TestTableFields.TEST_FIELD), Some(5d))
        .set(TimeExpr, Some(Time(qtime.plusHours(1))))
        .set(dimension(TestDims.DIM_A), Some("Foo"))
        .buildAndReset()
    )

    val copy = serialized(original)
    copy.hashCode() shouldEqual original.hashCode()
  }

  it should "support equals for serialized and not serialized instances" in {
    val qtime = LocalDateTime.of(2019, 10, 12, 13, 47).atOffset(ZoneOffset.UTC)

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        min(metric(TestTableFields.TEST_LONG_FIELD)) as "testField",
        truncDay(time) as "time",
        dimension(TestDims.DIM_A).toField,
        dimension(TestDims.DIM_B).toField
      ),
      None,
      Seq(dimension(TestDims.DIM_A), dimension(TestDims.DIM_B))
    )
    val context = new QueryContext(query, None, ExpressionCalculatorFactory)

    val builder = new InternalRowBuilder(context)

    val original = new KeyData(
      context,
      builder
        .set(metric(TestTableFields.TEST_LONG_FIELD), Some(42L))
        .set(TimeExpr, Some(Time(qtime.plusHours(1))))
        .set(dimension(TestDims.DIM_A), Some("foo"))
        .set(dimension(TestDims.DIM_B), Some("bar"))
        .buildAndReset()
    )

    val copy = serialized(original)
    copy shouldEqual original
    original shouldEqual copy

    val copy2 = serialized(original)
    copy2 shouldEqual copy
  }

  private def serialized[T <: Serializable](t: T): T = {
    val bos = new ByteArrayOutputStream()
    val os = new ObjectOutputStream(bos)
    os.writeObject(t)
    os.flush()
    val bytes = bos.toByteArray

    val is = new ObjectInputStream(new ByteArrayInputStream(bytes))
    is.readObject().asInstanceOf[T]
  }
}
