/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.core.model

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import org.yupana.api.Time
import org.yupana.api.query.{ Query, TimeExpr }
import org.yupana.core.{ QueryContext, TestDims, TestSchema, TestTableFields }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.core.jit.JIT

import java.time.{ LocalDateTime, ZoneOffset }
import org.yupana.core.utils.metric.NoMetricCollector

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
    val context = new QueryContext(query, None, JIT, NoMetricCollector)

    val builder = new InternalRowBuilder(context)

    val original = new KeyData(
      context,
      builder,
      builder
        .set(metric(TestTableFields.TEST_FIELD), 5d)
        .set(TimeExpr, Time(qtime.plusHours(1)))
        .set(dimension(TestDims.DIM_A), "Foo")
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
    val context = new QueryContext(query, None, JIT, NoMetricCollector)

    val builder = new InternalRowBuilder(context)

    val original = new KeyData(
      context,
      builder,
      builder
        .set(metric(TestTableFields.TEST_LONG_FIELD), 42L)
        .set(TimeExpr, Time(qtime.plusHours(1)))
        .set(dimension(TestDims.DIM_A), "foo")
        .set(dimension(TestDims.DIM_B), 2.toShort)
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
