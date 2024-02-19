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

package org.yupana.core
import org.yupana.api.query.{ DataPoint, Query, Result }
import org.yupana.core.auth.YupanaUser

class TimeSeriesQueryEngine(tsdb: TSDB) {
  def query(user: YupanaUser, query: Query): Result = {
    tsdb.query(query, user)
  }

  def put(user: YupanaUser, dps: Seq[DataPoint]): Unit = {
    tsdb.put(dps.iterator, user)
  }
}
