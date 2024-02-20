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

sealed trait Action

object Action {
  case object Read extends Action
  case object Write extends Action
}

sealed trait Subject

object Subject {
  case class Table(name: String) extends Subject
  case object User extends Subject
}

class PermissionService {
  def hasPermission(user: YupanaUser, subject: Subject, action: Action): Boolean = {
    user.role.permissions.exists(_.implies(subject, action))
  }
}
