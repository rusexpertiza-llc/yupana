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

object SchemaCheckResult {
  def empty: SchemaCheckResult = Success
  def combine(a: SchemaCheckResult, b: SchemaCheckResult): SchemaCheckResult = (a, b) match {
    case (Error(msg1), Error(msg2))     => Error(msg1 + "\n" + msg2)
    case (Error(msg1), Warning(msg2))   => Error(msg1 + "\n" + msg2)
    case (Warning(msg1), Error(msg2))   => Error(msg1 + "\n" + msg2)
    case (Warning(msg1), Warning(msg2)) => Warning(msg1 + "\n" + msg2)
    case (Success, Success)             => Success
    case (Success, notSuccess)          => notSuccess
    case (notSuccess, Success)          => notSuccess
  }
}
case object Success extends SchemaCheckResult
case class Warning(message: String) extends SchemaCheckResult
case class Error(message: String) extends SchemaCheckResult
