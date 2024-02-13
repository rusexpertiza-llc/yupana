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
  val priority: Int
}

object TsdbRole {
  case object Disabled extends TsdbRole {
    override val name: String = "DISABLED"
    override val priority: Int = 0
  }
  case object ReadOnly extends TsdbRole {
    override val name: String = "READ_ONLY"
    override val priority: Int = 10
  }
  case object ReadWrite extends TsdbRole {
    override val name: String = "READ_WRITE"
    override val priority: Int = 20
  }
  case object Admin extends TsdbRole {
    override val name: String = "ADMIN"
    override val priority: Int = 100
  }

  private val roles: Map[String, TsdbRole] = Seq(Disabled, ReadOnly, ReadWrite, Admin).map(r => r.name -> r).toMap

  def roleByName(name: String): Option[TsdbRole] = roles.get(name.toUpperCase)
}
