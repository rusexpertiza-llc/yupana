package org.yupana.utils

import org.yupana.api.utils.Transliterator

object KazakhTransliterator extends Transliterator {

  private val kazakhTable = Map(
    'а' -> "a",
    'ә' -> "e",
    'б' -> "b",
    'в' -> "v",
    'г' -> "g",
    'ғ' -> "r",
    'д' -> "d",
    'е' -> "e",
    'ё' -> "e",
    'ж' -> "zh",
    'з' -> "z",
    'и' -> "i",
    'й' -> "j",
    'к' -> "k",
    'қ' -> "k",
    'л' -> "l",
    'м' -> "m",
    'н' -> "n",
    'ң' -> "n",
    'о' -> "o",
    'ө' -> "u",
    'п' -> "p",
    'р' -> "r",
    'с' -> "s",
    'т' -> "t",
    'у' -> "u",
    'ұ' -> "o",
    'ү' -> "u",
    'ф' -> "f",
    'х' -> "h",
    'һ' -> "h",
    'ц' -> "c",
    'ч' -> "ch",
    'ш' -> "sh",
    'щ' -> "shch",
    'ъ' -> "",
    'ы' -> "y",
    'і' -> "i",
    'ь' -> "",
    'э' -> "e",
    'ю' -> "yu",
    'я' -> "ya",
    'А' -> "A",
    'Ә' -> "E",
    'Б' -> "B",
    'В' -> "V",
    'Г' -> "G",
    'Ғ' -> "R",
    'Д' -> "D",
    'Е' -> "E",
    'Ё' -> "E",
    'Ж' -> "ZH",
    'З' -> "Z",
    'И' -> "I",
    'Й' -> "J",
    'К' -> "K",
    'Қ' -> "K",
    'Л' -> "L",
    'М' -> "M",
    'Н' -> "N",
    'Ң' -> "N",
    'О' -> "O",
    'Ө' -> "U",
    'П' -> "P",
    'Р' -> "R",
    'С' -> "S",
    'Т' -> "T",
    'У' -> "U",
    'Ұ' -> "O",
    'Ү' -> "U",
    'Ф' -> "F",
    'Х' -> "H",
    'Һ' -> "H",
    'Ц' -> "C",
    'Ч' -> "CH",
    'Ш' -> "SH",
    'Щ' -> "SHCH",
    'Ъ' -> "",
    'Ы' -> "Y",
    'Ь' -> "",
    'І' -> "I",
    'Э' -> "E",
    'Ю' -> "YU",
    'Я' -> "YA"
  )

  private val transliterator = new TableTransliterator(kazakhTable)

  override def transliterate(s: String): String = transliterator.transliterate(s)
}
