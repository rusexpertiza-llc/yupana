package org.yupana.khipu

import org.yupana.api.schema.Schema

import java.nio.file.Path

class DB(path: Path, schema: Schema) {

  val tables: Map[String, KTable] = schema.tables.map {
    case (name, table) => name -> KTable.mapFile(path.resolve(table.name), table)
  }
}
