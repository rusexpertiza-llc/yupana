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

package org.yupana.api.schema

/**
  * Database schema
  * @param tables all tables defined in this schema and their names
  * @param rollups list of rollups available for this schema
  */
class Schema(val tables: Map[String, Table], val rollups: Seq[Rollup]) extends Serializable {

  /** Get table by name */
  def getTable(name: String): Option[Table] = tables.get(name)

  /**
    * Modifies table in this schema and returns updated schema
    * @param name table name
    * @param f function to change table
    * @return schema with updated table
    */
  def withTableUpdated(name: String)(f: Table => Table): Schema = {
    if (tables.contains(name)) {
      val newTables = tables.updated(name, f(tables(name)))
      new Schema(newTables, rollups)
    } else this
  }

  def withRollup(r: Rollup): Schema = {
    new Schema(tables, rollups :+ r)
  }
}

object Schema {

  /**
    * Creates table for sequence of tables.
    * @param tables tables in this schema
    * @return schema instance
    */
  def apply(tables: Seq[Table], rollups: Seq[Rollup]): Schema = new Schema(tables.map(t => t.name -> t).toMap, rollups)
}
