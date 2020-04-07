package org.yupana.externallinks.universal

import java.util.Properties

import org.flywaydb.core.Flyway
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers, OptionValues }
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.yupana.core.cache.CacheFactory
import org.yupana.externallinks.universal.JsonCatalogs.{ SQLExternalLink, SQLExternalLinkConfig }
import org.yupana.schema.{ Dimensions, SchemaRegistry }

class SQLSourcedCatalogServiceTest extends FlatSpec with Matchers with OptionValues with BeforeAndAfterAll {
  val dbUrl = "jdbc:h2:mem:yupana;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
  val dbUser = "test"
  val dbPass = "secret"

  private def createService(config: SQLExternalLinkConfig): SQLSourcedExternalLinkService = {
    val ds = new DriverManagerDataSource(
      config.connection.url,
      config.connection.username.orNull,
      config.connection.password.orNull
    )
    val jdbc = new JdbcTemplate(ds)

    val externalLink = SQLExternalLink(config, Dimensions.KKM_ID_TAG)

    new SQLSourcedExternalLinkService(externalLink, config.description, jdbc)
  }

  import org.yupana.api.query.syntax.All._

  "SQLSourcedCatalog" should "work as defined in json" in {

    val simpleCatalogJson = s"""{
              |  "externalLinks": [
              |   {
              |    "description" : {
              |      "source": "sql",
              |      "linkName": "TestCatalog",
              |      "dimensionName": "kkmId",
              |      "fieldsNames": ["f1", "f2"],
              |      "tables": ["receipt"]
              |    },
              |    "connection" : {
              |      "url": "$dbUrl",
              |      "username": "$dbUser",
              |      "password": "$dbPass"
              |    }
              |   }
              |  ]
              |}""".stripMargin
    val parsed = JsonExternalLinkDeclarationsParser.parse(SchemaRegistry.defaultSchema, simpleCatalogJson)
    parsed match {
      case Left(err) => throw new TestFailedException(err, 3)
      case _         =>
    }
    val externalLinkConfig = parsed.right.get.head
    val externalLinkService = createService(externalLinkConfig)
    val externalLink = externalLinkService.externalLink

    val values =
      externalLinkService.fieldValuesForDimValues(
        externalLinkConfig.description.fieldsNames,
        Set("123432345655", "123432345657")
      )

    values.get("123432345655", "f1").value shouldEqual "wer"
    values.get("123432345655", "f2").value shouldEqual "sdf"
    values.get("123432345657", "f1").value shouldEqual "rty"
    values.get("123432345657", "f2").value shouldEqual "fgh"

    val inCondition = externalLinkService.condition(
      and(
        in(link(externalLink, "f1"), Set("qwe", "ert")),
        in(link(externalLink, "f2"), Set("asd", "fgh"))
      )
    )

    inCondition shouldEqual in(dimension(externalLink.dimension), Set("123432345654"))

    val notInCondition = externalLinkService.condition(
      and(
        notIn(link(externalLink, "f1"), Set("qwe", "ert")),
        notIn(link(externalLink, "f2"), Set("asd", "fgh"))
      )
    )

    notInCondition shouldEqual notIn(
      dimension(externalLink.dimension),
      Set("123432345654", "123432345656", "123432345657")
    )
  }

  it should "work from json definition with custom fields mapping and relation" in {
    val complicatedCatalogJson = s"""{
                               |  "externalLinks": [
                               |    {
                               |     "description" : {
                               |      "source": "sql",
                               |      "linkName": "ComplicatedTestCatalog",
                               |      "dimensionName": "kkmId",
                               |      "fieldsNames": ["f1", "f2"],
                               |      "fieldsMapping": {
                               |        "kkmId": "t1.k",
                               |        "f1": "t1.ff1",
                               |        "f2": "t2.ff2"
                               |      },
                               |      "relation": "t1 JOIN t2 ON t1.t2_id = t2.id",
                               |      "tables": ["receipt"]
                               |     },
                               |     "connection" : {
                               |      "url": "$dbUrl",
                               |      "username": "$dbUser",
                               |      "password": "$dbPass"
                               |     }
                               |    }
                               |  ]
                               |}""".stripMargin
    val externalLinkConfig =
      JsonExternalLinkDeclarationsParser.parse(SchemaRegistry.defaultSchema, complicatedCatalogJson).right.get.head
    val externalLinkService = createService(externalLinkConfig)
    val externalLink = externalLinkService.externalLink

    val values =
      externalLinkService.fieldValuesForDimValues(
        externalLinkConfig.description.fieldsNames,
        Set("123432345655", "123432345657")
      )

    values.get("123432345655", "f1").value shouldEqual "hhh2"
    values.get("123432345655", "f2").value shouldEqual "ggg"
    values.get("123432345657", "f1").value shouldEqual "hhh3"
    values.get("123432345657", "f2").value shouldEqual "ggg3"

    val inCondition = externalLinkService.condition(
      and(
        in(link(externalLink, "f1"), Set("hhh", "hhh3")),
        in(link(externalLink, "f2"), Set("ggg2", "ggg3"))
      )
    )

    inCondition shouldEqual in(dimension(externalLink.dimension), Set("123432345657"))

    val notInCondition = externalLinkService.condition(
      and(
        notIn(link(externalLink, "f1"), Set("hhh", "hhh3")),
        notIn(link(externalLink, "f2"), Set("ggg2", "ggg3"))
      )
    )

    notInCondition shouldEqual notIn(
      dimension(externalLink.dimension),
      Set("123432345654", "123432345656", "123432345657")
    )
  }

  override protected def beforeAll(): Unit = {
    val flyway = Flyway.configure().dataSource(dbUrl, dbUser, dbPass).load()
    flyway.migrate()

    val props = new Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(props, "ns")
  }
}
