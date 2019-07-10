package org.yupana.schema.externallinks

import org.yupana.api.schema.{Dimension, ExternalLink}
import org.yupana.schema.Dimensions

trait RelatedItemsCatalog extends ExternalLink {
  val ITEM_FIELD = "item"
  val PHRASE_FIELDS = "phrase"

  override val linkName: String = "RelatedItemsCatalog"
  override val dimension: Dimension = Dimensions.ITEM_TAG

  override val fieldsNames: Set[String] = Set(
    ITEM_FIELD,
    PHRASE_FIELDS
  )
}

object RelatedItemsCatalog extends RelatedItemsCatalog
