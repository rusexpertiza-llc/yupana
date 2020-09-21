package org.yupana.utils

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{ FlatSpec, Matchers }

class OfdItemFixerTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  "OfdItemFixer" should "replace broken chars" in {
    val items = Table(
      ("item", "fixer"),
      ("гуляш соевый 50% ┬лсойка┬╗, 500г (россия) шт", "гуляш соевый 50% \"сойка\", 500г (россия) шт"),
      (
        "подушка ┬лклассик┬╗400х300мм наперник х.б. наполнитель ┬ллузга гречихи┬╗ шт",
        "подушка \"классик\"400х300мм наперник х.б. наполнитель \"лузга гречихи\" шт"
      ),
      ("семечки крупные ╣1", "семечки крупные №1"),
      ("суперфуд fresh&free d\":\"a╕┐", "суперфуд fresh&free d\":\"aё┐"),
      ("г╕ссер 0.3 мл в розлив", "гёссер 0.3 мл в розлив")
    )

    forAll(items) { (item, fixed) =>
      OfdItemFixer.fix(item) shouldEqual fixed
    }
  }
}
