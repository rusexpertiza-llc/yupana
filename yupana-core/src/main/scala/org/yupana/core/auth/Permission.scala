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

sealed trait Perm[+T] {
  def implies[TT >: T](t: TT): Boolean
}

case object All extends Perm[Nothing] {
  override def implies[TT >: Nothing](t: TT): Boolean = true
}

case class One[+T](value: T) extends Perm[T] {
  override def implies[TT >: T](t: TT): Boolean = t == value
}

case class Permission[S, A](subject: Perm[S], action: Perm[A]) {
  def implies(s: S, a: A): Boolean = subject.implies(s) && action.implies(a)
}

case class Permissions(
    tablePermissions: Seq[Permission[Option[String], Action]],
    userPermission: Seq[Perm[Action]],
    metaPermission: Seq[Perm[Action]],
    queryPermission: Seq[Perm[Action]]
) {
  def implies(subject: Object, action: Action): Boolean = {
    subject match {
      case Object.Table(name) => tablePermissions.exists(_.implies(name, action))
      case Object.User        => userPermission.exists(_.implies(action))
      case Object.Metadata    => metaPermission.exists(_.implies(action))
      case Object.Queries     => queryPermission.exists(_.implies(action))
    }
  }
}
