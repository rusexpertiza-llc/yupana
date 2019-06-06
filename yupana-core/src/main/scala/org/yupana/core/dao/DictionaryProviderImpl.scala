package org.yupana.core.dao

import org.yupana.api.schema.Dimension
import org.yupana.core.Dictionary

class DictionaryProviderImpl(dictionaryDao: DictionaryDao) extends DictionaryProvider {

  private var dictionaries = Map.empty[Dimension, Dictionary]

  override def dictionary(dim: Dimension): Dictionary = {
    dictionaries.get(dim) match {
      case Some(d) => d
      case None =>
        val d = new Dictionary(dim, dictionaryDao)
        dictionaries += dim -> d
        d
    }
  }
}
