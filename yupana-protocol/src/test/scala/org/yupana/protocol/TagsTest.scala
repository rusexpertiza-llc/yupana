package org.yupana.protocol

import org.scalatest.flatspec.AnyFlatSpec

class TagsTest extends AnyFlatSpec { // with Matchers {
  "Tags" should "be different" in {
    import scala.reflect.runtime.{ universe => ru }
    val mirror = ru.runtimeMirror(getClass.getClassLoader)

    val tpe = ru.typeOf[Tag]
    println(s"tpe: ${tpe.typeSymbol.companion.asModule}")

    tpe.typeSymbol.asClass.knownDirectSubclasses.foreach { sym =>
      println(s"$sym ${sym.isStatic} ${sym.isClass} ${sym.isModule}  ")

//      val obj = mirror.reflectClass(sym.asClass).instance
      val obj = mirror.reflect(sym)
      println(s"OS: ${obj}")

      println(s"C = ${sym} ${obj}")
    }
  }
}
