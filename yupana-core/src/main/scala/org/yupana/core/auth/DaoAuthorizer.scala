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

import at.favre.lib.crypto.bcrypt.BCrypt
import org.yupana.core.dao.UserDao

class DaoAuthorizer(userDao: UserDao, superUserName: Option[String], superUserPassword: Option[String])
    extends Authorizer {

  override def authorize(
      userName: Option[String],
      password: Option[String]
  ): Either[String, YupanaUser] = {
    userName.map(_.trim) match {
      case Some(name) =>
        if (superUserName.contains(name) && superUserPassword == password) {
          Right(YupanaUser(name, password, TsdbRole.Admin))
        } else {
          userDao
            .findUser(name)
            .filter(yu =>
              BCrypt.verifyer().verify(password.getOrElse("").toCharArray, yu.password.getOrElse("")).verified
            )
            .toRight("Invalid user or password")
        }
      case None => Left("User name must not be empty")
    }
  }
}
