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

import org.yupana.api.schema.DictionaryDimension

object Dimensions {
  val KKM_ID_TAG = DictionaryDimension("kkmId")
  val ITEM_TAG = ItemDimension("item")
  val CUSTOMER_TAG = DictionaryDimension("customer")
  val SHIFT_TAG = DictionaryDimension("shift")
  val OPERATION_TYPE_TAG = DictionaryDimension("operation_type")
  val OPERATOR_TAG = DictionaryDimension("operator")
  val POSITION_TAG = DictionaryDimension("position")
}
