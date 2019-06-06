package org.yupana.utils

object Transliterator {
  private val chars: Array[String] = (Char.MinValue to Char.MaxValue).map(_.toString).toArray

  private val russianTable = Map(
    'а' -> "a", 'б' -> "b", 'в' -> "v", 'г' -> "g", 'д' -> "d", 'е' -> "e", 'ё' -> "e",
    'ж' -> "zh", 'з' -> "z", 'и' -> "i", 'й' -> "j", 'к' -> "k",
    'л' -> "l", 'м' -> "m", 'н' -> "n", 'о' -> "o", 'п' -> "p",
    'р' -> "r", 'с' -> "s", 'т' -> "t", 'у' -> "u", 'ф' -> "f",
    'х' -> "h", 'ц' -> "c", 'ч' -> "ch", 'ш' -> "sh", 'щ' -> "shch",
    'ъ' -> "", 'ы' -> "y", 'ь' -> "", 'э' -> "e", 'ю' -> "yu", 'я' -> "ya",
    'А' -> "A", 'Б' -> "B", 'В' -> "V", 'Г' -> "G", 'Д' -> "D", 'Е' -> "E", 'Ё' -> "E",
    'Ж' -> "ZH", 'З' -> "Z", 'И' -> "I", 'Й' -> "J", 'К' -> "K",
    'Л' -> "L", 'М' -> "M", 'Н' -> "N", 'О' -> "O", 'П' -> "P",
    'Р' -> "R", 'С' -> "S", 'Т' -> "T", 'У' -> "U", 'Ф' -> "F",
    'Х' -> "H", 'Ц' -> "C", 'Ч' -> "CH", 'Ш' -> "SH", 'Щ' -> "SHCH",
    'Ъ' -> "", 'Ы' -> "Y", 'Ь' -> "", 'Э' -> "E", 'Ю' -> "YU", 'Я' -> "YA"
  )

  russianTable.foreach { case (c, s) => chars(c) = s }

  def transliterate(s: String): String = {
    val builder = new java.lang.StringBuilder(s.length * 2)
    s.foreach { c =>
      builder.append(chars(c))
    }
    builder.toString
  }
}
