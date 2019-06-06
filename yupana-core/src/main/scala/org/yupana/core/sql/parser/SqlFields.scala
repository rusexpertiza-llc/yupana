package org.yupana.core.sql.parser

sealed trait SqlFields

case class SqlFieldList(fields: Seq[SqlField]) extends SqlFields

case object SqlFieldsAll extends SqlFields
