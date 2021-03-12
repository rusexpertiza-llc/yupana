package org.yupana.externallinks.universal

import org.scalatest.Inside
import org.yupana.api.schema.Schema
import org.yupana.externallinks.universal.JsonCatalogs.{
  SQLExternalLinkConfig,
  SQLExternalLinkConnection,
  SQLExternalLinkDescription
}
import org.yupana.schema.Tables
import org.yupana.utils.{ OfdItemFixer, RussianTokenizer, RussianTransliterator }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JsonExternalLinkDeclarationsParserTest extends AnyFlatSpec with Matchers with Inside {

  val testSchema = Schema(
    Seq(Tables.itemsKkmTable, Tables.kkmItemsTable, Tables.receiptTable),
    Seq.empty,
    OfdItemFixer,
    RussianTokenizer,
    RussianTransliterator
  )

  "JsonCatalogDeclarationsParser" should "parse good declarations" in {
    val json = s"""{
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
                  |    },
                  |    "connection" : {
                  |      "url": "jdbc:postgresql://host:5432/my_db",
                  |      "username": "root",
                  |      "password": "root"
                  |    }
                  |   }
                  |  ]
                  |}""".stripMargin

    inside(JsonExternalLinkDeclarationsParser.parse(testSchema, json)) {
      case Right(Seq(catalog)) =>
        catalog shouldEqual SQLExternalLinkConfig(
          description = SQLExternalLinkDescription(
            linkName = "ComplicatedTestCatalog",
            dimensionName = "kkmId",
            fieldsNames = Set("f1", "f2"),
            fieldsMapping = Some(Map("kkmId" -> "t1.k", "f1" -> "t1.ff1", "f2" -> "t2.ff2")),
            relation = Some("t1 JOIN t2 ON t1.t2_id = t2.id"),
            tables = Seq("receipt")
          ),
          connection = SQLExternalLinkConnection(
            url = "jdbc:postgresql://host:5432/my_db",
            username = Some("root"),
            password = Some("root")
          )
        )
    }
  }

  it should "return error if externalLinks is not defined" in {

    val noArray = "{}".stripMargin

    val e = JsonExternalLinkDeclarationsParser.parse(testSchema, noArray).left.get
    e shouldEqual s"No 'externalLinks' array was found in $noArray"
  }

  it should "return error if externalLinks is not an array" in {

    val notArray = s"""{
                              |  "externalLinks":
                              |    {
                              |      "source": "sql",
                              |      "linkName": "ComplicatedTestCatalog",
                              |      "url": "jdbc:postgresql://host:5432/my_db",
                              |      "username": "root",
                              |      "password": "root",
                              |      "dimensionName": "kkmId",
                              |      "fieldsNames": ["f1", "f2"],
                              |      "fieldsMapping": {
                              |        "kkmId": "t1.k",
                              |        "f1": "t1.ff1",
                              |        "f2": "t2.ff2"
                              |      },
                              |      "relation": "t1 JOIN t2 ON t1.t2_id = t2.id"
                              |    }
                              |}""".stripMargin

    val e2 = JsonExternalLinkDeclarationsParser.parse(testSchema, notArray).left.get
    e2 shouldEqual s"No 'externalLinks' array was found in $notArray"
  }

  it should "return errors if externalLinks defined incorrectly" in {

    val badLinks = s"""{
                          |  "externalLinks": [
                          |    {
                          |     "description" : {
                          |      "source": "sql",
                          |      "linkName": "GoodCatalog",
                          |      "dimensionName": "kkmId",
                          |      "fieldsNames": ["f1", "f2"],
                          |      "tables": ["receipt"]
                          |     },
                          |     "connection" : {
                          |      "url": "jdbc:postgresql://host:5432/my_db",
                          |      "username": "root",
                          |      "password": "root"
                          |     }
                          |    },
                          |    {
                          |      "description" : {
                          |        "source" : "bad source"
                          |      }
                          |    },
                          |    {
                          |      "description" : {
                          |        "source": "sql",
                          |        "linkName": "CaseClassExtractionError"
                          |      }
                          |    },
                          |    {
                          |     "description" : {
                          |      "source": "sql",
                          |      "linkName": "BadFieldsMapping1",
                          |      "dimensionName": "kkmId",
                          |      "fieldsNames": ["f1", "f2"],
                          |      "fieldsMapping": {
                          |        "kkmId": "kkmId",
                          |        "f2": "ff2"
                          |      },
                          |      "tables": ["receipt"]
                          |     },
                          |     "connection" : {
                          |       "url": "jdbc:postgresql://host:5432/my_db",
                          |      "username": "root",
                          |      "password": "root"
                          |     }
                          |    },
                          |    {
                          |     "description" : {
                          |      "source": "sql",
                          |      "linkName": "BadFieldsMapping2",
                          |      "dimensionName": "kkmId",
                          |      "fieldsNames": ["f1", "f2"],
                          |      "fieldsMapping": {
                          |        "kkmId": "kkmId",
                          |        "f1": "ff2",
                          |        "f2": "ff2"
                          |      },
                          |      "tables": ["receipt"]
                          |     },
                          |     "connection" : {
                          |      "url": "jdbc:postgresql://host:5432/my_db",
                          |      "username": "root",
                          |      "password": "root"
                          |     }
                          |    },
                          |    {
                          |     "description" : {
                          |      "source": "sql",
                          |      "linkName": "NoSchemas",
                          |      "dimensionName": "kkmId",
                          |      "fieldsNames": ["f1", "f2"]
                          |     },
                          |     "connection" : {
                          |      "url": "jdbc:postgresql://host:5432/my_db",
                          |      "username": "root",
                          |      "password": "root"
                          |     }
                          |    },
                          |    {
                          |     "description" : {
                          |      "source": "sql",
                          |      "linkName": "BadSchemas",
                          |      "dimensionName": "kkmId",
                          |      "fieldsNames": ["f1", "f2"],
                          |      "tables": ["kkm_items", "items_kkms"]
                          |     },
                          |     "connection" : {
                          |      "url": "jdbc:postgresql://host:5432/my_db",
                          |      "username": "root",
                          |      "password": "root"
                          |     }
                          |    }
                          |  ]
                          |}""".stripMargin

    val e3 = JsonExternalLinkDeclarationsParser.parse(testSchema, badLinks).left.get
    e3 shouldEqual Seq(
      """Bad source field in {"description":{"source":"bad source"}}""",
      """Can not parse external link {"description":{"source":"sql","linkName":"CaseClassExtractionError"}}: """ +
        "No usable value for description\nNo usable value for dimensionName\nDid not find value which can be converted into java.lang.String",
      "Fields mapping keys set is not equal to declared external link fields set: " +
        "Set(kkmId, f2) != Set(f1, f2, kkmId) in 'BadFieldsMapping1'",
      "Inverse fields mapping contains duplicated keys: List(kkmId, ff2, ff2) in 'BadFieldsMapping2'",
      "No tables defined for external link NoSchemas",
      "Unknown table: items_kkms in 'BadSchemas'"
    ).mkString(", ")
  }
}
