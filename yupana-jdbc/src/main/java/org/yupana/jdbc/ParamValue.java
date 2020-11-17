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

import java.math.BigDecimal;
import java.sql.Timestamp;

public class ParamValue {

    private BigDecimal numericValue;
    private Timestamp timestampValue;
    private String stringValue;
    private final ParamValueType type;

    public ParamValue(String string) {
        stringValue = string;
        type = ParamValueType.STRING;
    }

    public ParamValue(Timestamp timestamp) {
        timestampValue = timestamp;
        type = ParamValueType.TIMESTAMP;
    }

    public ParamValue(BigDecimal decimal) {
        numericValue = decimal;
        type = ParamValueType.NUMERIC;
    }

    public ParamValueType getType() {
        return type;
    }

    public String getStringValue() {
        return stringValue;
    }

    public BigDecimal getNumericValue() {
        return numericValue;
    }

    public Timestamp getTimestampValue() {
        return timestampValue;
    }
}
