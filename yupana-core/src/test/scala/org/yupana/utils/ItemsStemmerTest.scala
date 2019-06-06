package org.yupana.utils

import org.scalatest.{FlatSpec, Matchers}

class ItemsStemmerTest extends FlatSpec with Matchers {
  "Stemmer" should "split numbers and words" in {
    ItemsStemmer.words("95пульсар") should contain theSameElementsAs List("95пульсар", "95", "пульсар")

    ItemsStemmer.words("аи95пульсар") should contain theSameElementsAs List("аи95пульсар", "аи", "95", "пульсар")

    ItemsStemmer.words("аи95") should contain theSameElementsAs List("аи95", "аи", "95")

    ItemsStemmer.words("трк 1 (atum-92-к5) 1.256281 x 39.80") should contain theSameElementsAs List("трк", "1", "atum", "92", "к", "5", "к5", "1", "256281", "1.256281", "x", "39", "80", "39.80")
  }

  it should "keep the space" in {
    ItemsStemmer.words(" лаки дейз ж/р арбуз подушечки 14г (c") should contain allOf ("дейз", "ж/р")

    ItemsStemmer.words("мясн/пр сос.классич с сливк. и/о вар 0,4кг пл/у(аг") should contain ("и/о")
  }

  it should "preserve ai" in {
    ItemsStemmer.words("аи") should contain theSameElementsAs List("аи")

    ItemsStemmer.words("аи-95") should contain theSameElementsAs List("аи", "95")

    ItemsStemmer.words("аи-95аи-95-к5") should contain theSameElementsAs List("аи", "95а", "95", "аи", "95", "к5", "к", "5")

    ItemsStemmer.words("бензин аи95 n 3") should contain theSameElementsAs List("бензин", "аи", "95", "аи95", "n", "3")

    ItemsStemmer.words("95аи") should contain theSameElementsAs List("аи", "95", "95а")
  }

  it should "preserve ai with english 'a' and russian 'и' letters" in {
    ItemsStemmer.words("aи") should contain theSameElementsAs List("aи")

    ItemsStemmer.words("aи-95") should contain theSameElementsAs List("aи", "95")

    ItemsStemmer.words("aи-95aи-95-к5") should contain theSameElementsAs List("aи", "95a", "95", "aи", "95", "к5", "к", "5")

    ItemsStemmer.words("бензин aи95 n 3") should contain theSameElementsAs List("бензин", "aи", "95", "aи95", "n", "3")
  }

  it should "preserve letter slash letter abbreviations" in {
    ItemsStemmer.words(";;&х/к") should contain theSameElementsAs List("х", "к", "х/к")

    ItemsStemmer.words("п/п/к") should contain theSameElementsAs List("п", "п", "п/п", "к", "п/п/к")

    ItemsStemmer.words("х/к") should contain theSameElementsAs List("х", "к", "х/к")

    ItemsStemmer.words("qг/qк") should contain theSameElementsAs List("qг", "qк", "qг/qк")

    ItemsStemmer.words("пы/п/к") should contain theSameElementsAs List("пы", "п", "к", "пы/п", "пы/п/к")

    ItemsStemmer.words("п/пы/к") should contain theSameElementsAs List("пы", "п", "к", "п/п", "п/пы/к")

    ItemsStemmer.words("п/п/кы") should contain theSameElementsAs List("кы", "п", "п", "п/п", "п/п/к")

    ItemsStemmer.words("Д/Т") should contain theSameElementsAs List("д", "т", "д/т")

    ItemsStemmer.words("д/т n 1") should contain theSameElementsAs List("д/т", "т", "д", "n", "1")

    ItemsStemmer.words("\"дт фора/-32, n 1:00000\"") should contain theSameElementsAs List("дт", "фор", "32", "n", "1", "00000")

    ItemsStemmer.words("\"дт фора/32, n 1:00000\"") should contain theSameElementsAs List("дт", "фор", "32", "фора/32", "n", "1", "00000")

    ItemsStemmer.words("трk 1:аи-95пульсар-k5 трз0") should contain theSameElementsAs List("трk", "1", "аи", "95пульсар", "95", "пульсар", "k", "5", "k5", "трз0", "трз", "0")

    ItemsStemmer.words("\"дт* фора/-32, n 1:00000\"") should contain theSameElementsAs List("дт", "фор", "32", "n", "1", "00000")

    ItemsStemmer.words("д*т") should contain theSameElementsAs List("д", "т")

    ItemsStemmer.words("д*/т") should contain theSameElementsAs List("д", "т", "/т")
  }

  it should "real test" in {
    var result: Seq[String] = null

    result = ItemsStemmer.words("*стельки(шт)")
    result should contain theSameElementsAs List("стельк", "шт")

    result = ItemsStemmer.words("\"Ж/р\"\"Ригли\"\"джусифрут минис  15,9г\"")
    result should contain theSameElementsAs List("ж", "р", "ж/р", "ригл", "джусифрут", "минис", "15", "9", "г", "15,9г")

    result = ItemsStemmer.words("\"АКЦИЯ! Сухарики \"\"ХруcТим багет\"\" 60г\"")
    result should contain theSameElementsAs List("акц", "сухарик", "хруcт", "багет", "60", "г", "60г")

    result = ItemsStemmer.words("\"Набор: подсвечник \"\"Звезда\"\" + с\"")
    result should contain theSameElementsAs List("набор", "подсвечник", "звезд", "с")
  }
}
