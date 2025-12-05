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

package org.yupana.api.schema

trait SchemaChecker {
  def check(schema: Schema, expectedSchema: Array[Byte]): SchemaCheckResult
  def toBytes(schema: Schema): Array[Byte]
}

sealed trait SchemaCheckResult
case class SchemaCheckSucceed(messages: List[SchemaCheckMessage] = Nil) extends SchemaCheckResult
case class SchemaCheckFailed(messages: List[SchemaCheckMessage]) extends SchemaCheckResult

object SchemaCheckResult {
  def empty: SchemaCheckResult = SchemaCheckSucceed(Nil)
  def combine(a: SchemaCheckResult, b: SchemaCheckResult): SchemaCheckResult = (a, b) match {
    case (SchemaCheckFailed(ms1), SchemaCheckFailed(ms2))   => SchemaCheckFailed(ms1 ++ ms2)
    case (SchemaCheckSucceed(ms1), SchemaCheckSucceed(ms2)) => SchemaCheckSucceed(ms1 ++ ms2)
    case (SchemaCheckSucceed(ms1), SchemaCheckFailed(ms2))  => SchemaCheckFailed(ms1 ++ ms2)
    case (SchemaCheckFailed(ms1), SchemaCheckSucceed(ms2))  => SchemaCheckFailed(ms1 ++ ms2)
  }
}

sealed trait SchemaCheckMessage
case class Warning(message: String) extends SchemaCheckMessage
case class Error(message: String) extends SchemaCheckMessage
