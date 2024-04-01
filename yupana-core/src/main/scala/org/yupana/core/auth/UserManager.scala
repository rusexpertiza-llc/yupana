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
import org.yupana.core.auth.UserManager.verifyPassword
import org.yupana.core.dao.UserDao

class UserManager(userDao: UserDao, superUserName: Option[String], superUserPassword: Option[String]) {

  def createUser(userName: String, password: Option[String], role: Option[String]): Either[String, YupanaUser] = {
    for {
      r <- role.map(r => getRole(r)).getOrElse(Right(TsdbRole.Disabled))
      hash = Some(UserManager.hashPassword(password.getOrElse("")))
      user <- Either.cond(userDao.createUser(userName, hash, r), YupanaUser(userName, hash, r), "User already exists")
    } yield user
  }
  def updateUser(userName: String, password: Option[String], role: Option[String]): Either[String, Unit] = {
    for {
      r <- role.fold(Right(None): Either[String, Option[TsdbRole]])(x => getRole(x).map(Some(_)))
      s <- Either.cond(userDao.updateUser(userName, password.map(UserManager.hashPassword), r), (), "User not found")
    } yield s
  }
  def deleteUser(userName: String): Boolean = userDao.deleteUser(userName)

  def findUser(userName: String): Option[YupanaUser] = userDao.findUser(userName)

  def listUsers(): List[YupanaUser] = userDao.listUsers()

  def validateUser(userName: String, password: Option[String]): Option[YupanaUser] = {
    if (superUserName.contains(userName) && superUserPassword == password) {
      Some(YupanaUser(userName, password, TsdbRole.Admin))
    } else {
      userDao
        .findUser(userName)
        .filter(x => verifyPassword(password.getOrElse(""), x.password.getOrElse("")))
    }
  }

  private def getRole(name: String): Either[String, TsdbRole] = {
    TsdbRole.roleByName(name).toRight(s"Invalid role name '$name'")
  }
}

object UserManager {

  private val HASH_COST = 12
  def hashPassword(password: String): String = {
    BCrypt.withDefaults().hashToString(HASH_COST, password.toCharArray)
  }

  def verifyPassword(password: String, hash: String): Boolean = {
    BCrypt.verifyer().verify(password.toCharArray, hash).verified
  }
}
