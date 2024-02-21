package org.yupana.core.auth

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PermissionServiceTest extends AnyFlatSpec with Matchers {
  "PermissionService" should "check permissions" in {
    val service = new PermissionService(true)

    val user1 = YupanaUser("Test1", None, TsdbRole.ReadOnly)
    service.hasPermission(user1, Subject.Table(Some("items_kkm")), Action.Read) shouldBe true
    service.hasPermission(user1, Subject.Table(Some("items_kkm")), Action.Write) shouldBe false

    service.hasPermission(user1, Subject.User, Action.Read) shouldBe false
    service.hasPermission(user1, Subject.User, Action.Write) shouldBe false

    service.hasPermission(user1, Subject.Queries, Action.Read) shouldBe true
    service.hasPermission(user1, Subject.Queries, Action.Write) shouldBe false

    val user2 = YupanaUser("Test1", None, TsdbRole.ReadWrite)
    service.hasPermission(user2, Subject.Table(Some("items_kkm")), Action.Read) shouldBe true
    service.hasPermission(user2, Subject.Table(Some("items_kkm")), Action.Write) shouldBe true

    service.hasPermission(user2, Subject.User, Action.Read) shouldBe false
    service.hasPermission(user2, Subject.User, Action.Write) shouldBe false

    service.hasPermission(user2, Subject.Queries, Action.Read) shouldBe true
    service.hasPermission(user2, Subject.Queries, Action.Write) shouldBe false
  }

  it should "not allow write when put disabled" in {
    val service = new PermissionService(false)

    val user = YupanaUser("Test1", None, TsdbRole.ReadWrite)
    service.hasPermission(user, Subject.Table(Some("items_kkm")), Action.Read) shouldBe true
    service.hasPermission(user, Subject.Table(Some("items_kkm")), Action.Write) shouldBe false

    service.hasPermission(user, Subject.User, Action.Read) shouldBe false
    service.hasPermission(user, Subject.User, Action.Write) shouldBe false

    service.hasPermission(user, Subject.Queries, Action.Read) shouldBe true
    service.hasPermission(user, Subject.Queries, Action.Write) shouldBe false
  }
}
