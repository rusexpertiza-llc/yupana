package org.yupana.core.model

import org.yupana.api.Time

case class TimeSensitiveFieldValues(time: Time, fieldValues: Map[String, Any])
