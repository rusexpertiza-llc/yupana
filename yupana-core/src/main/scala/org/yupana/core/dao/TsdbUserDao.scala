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

package org.yupana.core.dao

sealed trait TsdbRole

case object Disabled extends TsdbRole
case object ReadOnly extends TsdbRole
case object ReadWrite extends TsdbRole
case object ReadAdmin extends TsdbRole

trait TsdbUserDao {
  def createUser(userName: String, password: Option[String], role: TsdbRole): Unit
  def updateUser(userName: String, password: Option[String], role: Option[TsdbRole]): Unit
  def deleteUser(userName: String): Unit

  def findUser(userName: String): Option[String]

  def listUsers(): List[String]
}
