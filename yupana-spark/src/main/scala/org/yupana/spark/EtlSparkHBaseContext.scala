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

package org.yupana.spark

import org.yupana.api.query.Query
import org.yupana.api.schema.Schema
import org.yupana.core.TSDB
import org.yupana.etl.EtlHBaseContext
import org.yupana.hbase.TSDBHBase

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

class EtlSparkHBaseContext(cfg: EtlSparkHBaseConfig, schema: Schema)
    extends EtlHBaseContext(cfg, schema)
    with Serializable {

  private def initTsdb: TSDB = {
    val tsdb = TSDBHBase(cfg.tsdbConfig, schema, identity[Query] _, None)
    setup(tsdb)
    EtlSparkHBaseContext.tsdb = Some(tsdb)
    tsdb
  }

  @transient override lazy val tsdb: TSDB = EtlSparkHBaseContext.tsdb.getOrElse(initTsdb)
}

object EtlSparkHBaseContext {
  private var tsdb: Option[TSDB] = None
}
