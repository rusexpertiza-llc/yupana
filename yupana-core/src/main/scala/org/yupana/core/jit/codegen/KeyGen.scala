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

package org.yupana.core.jit.codegen

import org.yupana.api.query.Query
import org.yupana.core.jit.ValueDeclaration
import org.yupana.core.model.DatasetSchema

import scala.reflect.runtime.universe._

object KeyGen {
  def mkHashCode(query: Query, schema: DatasetSchema): Tree = {
    val trees = query.groupBy.zipWithIndex.map {
      case (expr, i) =>
        val valDecl = ValueDeclaration(s"keyField_$i")
        val read = BatchDatasetGen.mkGet(schema, expr, TermName("ds"), q"rowNum", valDecl)
        val mixfunc = if (i == 0) {
          q"valueHash"
        } else if (i < query.groupBy.size - 1) {
          q"scala.util.hashing.MurmurHash3.mix(h, valueHash)"
        } else {
          q"scala.util.hashing.MurmurHash3.finalizeHash(scala.util.hashing.MurmurHash3.mix(h, valueHash), ${query.groupBy.size})"
        }
        q"""
          {
            ..$read
            val valueHash = if (${valDecl.validityFlagName}) ${valDecl.valueName}.hashCode() else 0
            h = $mixfunc
          }
        """
    }

    if (trees.nonEmpty) {
      q"""
         var h = 0
         ..$trees
         h
       """
    } else {
      q"0"
    }
  }

  def mkEquals(query: Query, schema: DatasetSchema): Tree = {
    val trees = query.groupBy.zipWithIndex.map {
      case (expr, i) =>
        val valDecl1 = ValueDeclaration(s"keyField1_$i")
        val valDecl2 = ValueDeclaration(s"keyField2_$i")

        val read1 = BatchDatasetGen.mkGet(schema, expr, TermName("thatDs"), q"that.asInstanceOf[Key].rowNum", valDecl1)
        val read2 = BatchDatasetGen.mkGet(schema, expr, TermName("ds"), q"rowNum", valDecl2)
        q"""
          val thatDs = that.asInstanceOf[Key].ds
          ..$read1
          ..$read2
          r = r && (${valDecl1.validityFlagName} == ${valDecl2.validityFlagName}) && (${valDecl1.valueName} == ${valDecl2.valueName})
        """
    }

    if (trees.nonEmpty) {
      q"""
         var r = true
         ..$trees
         r
       """
    } else {
      q"true"
    }
  }
}
