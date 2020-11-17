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

package org.yupana.jdbc;

import java.sql.Connection;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.yupana.Proto.Version;
import org.yupana.api.query.Result;

interface YupanaConnection extends Connection {
  Result runQuery(String query, Map<Integer, ParamValue> params);
  Result runBatchQuery(String query, Collection<Map<Integer, ParamValue>> params);
  Optional<Version> serverVersion();
  String url();
}
