package org.yupana.protocol

import org.scalatest.Inspectors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TagsTest extends AnyFlatSpec with Matchers with Inspectors {
  "Tags" should "be different" in {
    import scala.reflect.runtime.{ universe => ru }
    val mirror = ru.runtimeMirror(getClass.getClassLoader)

    val tpe = ru.typeOf[Tag]

    val tags = tpe.typeSymbol.asClass.knownDirectSubclasses
      .map { sym =>

        mirror
          .reflectClass(sym.asClass)
          .reflectConstructor(sym.asClass.primaryConstructor.asMethod)()
          .asInstanceOf[Tag]
      }
      .groupBy(_.value.toChar)

    forAll(tags) {
      case (_, ts) =>
        ts should have size 1
    }

  }
}
