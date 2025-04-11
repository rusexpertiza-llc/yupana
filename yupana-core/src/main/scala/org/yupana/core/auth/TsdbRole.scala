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

package org.yupana.core.auth

sealed trait TsdbRole {
  val name: String
  val permissions: Permissions
}

object TsdbRole {
  case object Disabled extends TsdbRole {
    override val name: String = "DISABLED"
    override val permissions: Permissions = Permissions(Nil, Nil, Nil, Nil)
  }
  case object ReadOnly extends TsdbRole {
    override val name: String = "READ_ONLY"
    override val permissions: Permissions =
      Permissions(Seq(Permission(All, One(Action.Read))), Nil, Seq(All), Seq(One(Action.Read)))
  }
  case object ReadWrite extends TsdbRole {
    override val name: String = "READ_WRITE"
    override val permissions: Permissions =
      Permissions(
        Seq(Permission(All, One(Action.Read)), Permission(All, One(Action.Write))),
        Nil,
        Seq(All),
        Seq(One(Action.Read))
      )
  }
  case object Admin extends TsdbRole {
    override val name: String = "ADMIN"
    override val permissions: Permissions = Permissions(Seq(Permission(All, All)), Seq(All), Seq(All), Seq(All))
  }

  private val roles: Map[String, TsdbRole] = Seq(Disabled, ReadOnly, ReadWrite, Admin).map(r => r.name -> r).toMap

  def roleByName(name: String): Option[TsdbRole] = roles.get(name.toUpperCase)
}
