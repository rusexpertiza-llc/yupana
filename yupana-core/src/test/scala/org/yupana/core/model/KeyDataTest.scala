package org.yupana.core.model

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }

import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.Time
import org.yupana.api.query.{ Query, TimeExpr }
import org.yupana.core.{ QueryContext, TestDims, TestSchema, TestTableFields }

class KeyDataTest extends FlatSpec with Matchers {

  import org.yupana.api.query.syntax.All._

  "KeyData" should "preserve hash after serialization" in {
    val qtime = new LocalDateTime(2019, 10, 12, 13, 47).toDateTime(DateTimeZone.UTC)

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        sum(metric(TestTableFields.TEST_FIELD)) as "testField",
        truncDay(time) as "time",
        dimension(TestDims.TAG_A) as "TAG_A"
      ),
      None,
      Seq(dimension(TestDims.TAG_A))
    )
    val context = QueryContext(query, const(true))

    val builder = new InternalRowBuilder(context)

    val original = new KeyData(
      context,
      builder
        .set(metric(TestTableFields.TEST_FIELD), Some(5d))
        .set(TimeExpr, Some(Time(qtime.plusHours(1))))
        .set(dimension(TestDims.TAG_A), Some("Foo"))
        .buildAndReset()
    )

    val copy = serialized(original)
    copy.hashCode() shouldEqual original.hashCode()
  }

  it should "support equals for serialized and not serialized instances" in {
    val qtime = new LocalDateTime(2019, 10, 12, 13, 47).toDateTime(DateTimeZone.UTC)

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        min(metric(TestTableFields.TEST_LONG_FIELD)) as "testField",
        truncDay(time) as "time",
        dimension(TestDims.TAG_A).toField,
        dimension(TestDims.TAG_B).toField
      ),
      None,
      Seq(dimension(TestDims.TAG_A), dimension(TestDims.TAG_B))
    )
    val context = QueryContext(query, const(true))

    val builder = new InternalRowBuilder(context)

    val original = new KeyData(
      context,
      builder
        .set(metric(TestTableFields.TEST_LONG_FIELD), Some(42L))
        .set(TimeExpr, Some(Time(qtime.plusHours(1))))
        .set(dimension(TestDims.TAG_A), Some("foo"))
        .set(dimension(TestDims.TAG_B), Some("bar"))
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
