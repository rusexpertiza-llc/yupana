package org.yupana.schema.externallinks

import org.yupana.api.schema.{Dimension, ExternalLink}
import org.yupana.schema.Dimensions

trait ItemsInvertedIndex extends ExternalLink {
  val PHRASE_FIELD = "phrase"
  override val linkName: String = "ItemsInvertedIndex"
  override val dimension: Dimension = Dimensions.ITEM_TAG
  override val fieldsNames: Set[String] = Set(PHRASE_FIELD)
}

object ItemsInvertedIndex extends ItemsInvertedIndex
