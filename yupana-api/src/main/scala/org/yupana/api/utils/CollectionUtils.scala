package org.yupana.api.utils

object CollectionUtils {
  def mkStringWithLimit(seq: Iterable[_], limit: Int = 10, start: String = "[", sep: String = ", ", end: String = "]"): String = {
    val length = seq.size
    if (length <= limit) {
      seq.mkString(start, sep, end)
    } else {
      val firstN = seq.take(limit).mkString(start, sep, "")
      s"$firstN$sep... and ${length - limit} more$end"
    }
  }
}
