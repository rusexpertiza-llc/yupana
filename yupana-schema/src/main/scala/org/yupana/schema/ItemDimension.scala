package org.yupana.schema

import org.yupana.api.schema.Dimension
import org.yupana.utils.{ItemsStemmer, Transliterator}

object ItemDimension {

  def stem(text: String): Seq[String] = {
    ItemsStemmer.words(text).map(Transliterator.transliterate)
  }

  def apply(name:String): Dimension = {
    Dimension(name, Some(hash))
  }

  val stopWords: Set[String] = Set("kg", "ml", "lit","litr", "gr", "sht")
  val numOfChars: Int = 4

  private val bs: List[String] = List("skdgnl", "pmfzrc", "tboi", "vaei", "jq", "uw", "x", "y")
  private val charIdx8: Map[Char, Int] = bs.zipWithIndex.flatMap { case (b, i) => b.map(c => (c, i)) }.toMap

  def hash(item: String): Int = {

    val stemmed = stem(item).toArray

    val words = stemmed.filterNot(stopWords.contains).filter(i => i.length > 1 && i.forall(_.isLetter))

    (0 until numOfChars).foldLeft(0) { (h, pos) =>

      val code = if (pos == 0) {
        encode(chars(words, pos), charIdx8, 8)
      } else {
        math.abs(new String(chars(words, pos-1).sorted).hashCode) % 255
      }
      (h << 8) | code
    }
  }

  private def encode(chars: Array[Char], charIdx: Map[Char, Int], nBits: Int) = {
    val bits = chars.map(c => charIdx(c) % nBits)
    bits.foldLeft(0){ (a, i) =>
      (1 << i) | a
    }
  }

  private def chars(words: Array[String], pos: Int) = {
    words.map { w =>
      if (w.length > pos) w(pos) else ' '
    }.filter(charIdx8.contains)
  }
}
