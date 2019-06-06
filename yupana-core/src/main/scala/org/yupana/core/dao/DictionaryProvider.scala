package org.yupana.core.dao

import org.yupana.api.schema.Dimension
import org.yupana.core.Dictionary

trait DictionaryProvider {
  def dictionary(dimension: Dimension): Dictionary
}
