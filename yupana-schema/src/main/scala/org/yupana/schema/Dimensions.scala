/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
