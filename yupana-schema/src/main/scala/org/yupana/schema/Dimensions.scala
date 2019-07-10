package org.yupana.schema

import org.yupana.api.schema.Dimension

object Dimensions {
  val KKM_ID_TAG = Dimension("kkmId")
  val ITEM_TAG = ItemDimension("item")
  val CUSTOMER_TAG = Dimension("customer")
  val SHIFT_TAG = Dimension("shift")
  val OPERATION_TYPE_TAG = Dimension("operation_type")
  val OPERATOR_TAG = Dimension("operator")
  val POSITION_TAG = Dimension("position")
}
