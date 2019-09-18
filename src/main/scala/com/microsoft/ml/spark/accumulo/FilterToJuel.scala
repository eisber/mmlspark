// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.spark.sql.sources._

case class AccumuloFilterResult(val serializedFilter: String,
																val supportedFilters: Seq[Filter],
																val unsupportedFilters: Seq[Filter])

object FilterToJuel {

	def serializeValue(value: Any): String = {
		value match {
			case str: String => {
				// properly escape \ and '
				val strEscaped = str
					.replace("\\", "\\\\")
  				.replace("'", "\\'")

				"'" + strEscaped + "'"
			}
			case other: Any => other.toString
		}
	}

	def serializeFilter(filter: Filter): String = {
		filter match {
			case op: And => s"(${serializeFilter(op.left)} && ${serializeFilter(op.right)})"
			case op: Or => s"(${serializeFilter(op.left)} || ${serializeFilter(op.right)})"
			case op: EqualTo =>  s"(${op.attribute} == ${serializeValue(op.value)})"
			case op: GreaterThan => s"(${op.attribute} > ${serializeValue(op.value)})"
			case op: GreaterThanOrEqual => s"(${op.attribute} >= ${serializeValue(op.value)})"
			case op: LessThan => s"(${op.attribute} < ${serializeValue(op.value)})"
			case op: LessThanOrEqual => s"(${op.attribute} <= ${serializeValue(op.value)})"
			case op: Not => s"(!${serializeFilter(op.child)})"
			case op: IsNull => s"(${op.attribute} == null)"
			case op: IsNotNull => s"(${op.attribute} != null)"
			case op: StringContains => s"${op.attribute}.contains(${serializeValue(op.value)})"
			case op: StringStartsWith => s"${op.attribute}.startsWith(${serializeValue(op.value)})"
			case op: StringEndsWith => s"${op.attribute}.endsWith(${serializeValue(op.value)})"
			case op: In => {
				val values = op.values.map { v => serializeValue(v) } .mkString(",")
				s"${op.attribute}.in(${values})"
			}
			// TODO: not sure if null handling is properly done
			// TODO:  EqualNullSafe
			case _ => throw new UnsupportedOperationException(s"Filter ${filter} not supported")
		}
	}

	def serializeFilters(filters: Array[Filter]): AccumuloFilterResult = {
		val (supported, unsupported) = filters.map({ f => {

			try {
				(serializeFilter(f), f)
			} catch {
				case e: UnsupportedOperationException => ("", f)
			}
		}}).partition(!_._1.isEmpty)

		val filter = supported.map(_._1).mkString(" && ")

		AccumuloFilterResult(
			"${" + filter + "}",
			supported.map(_._2),
			unsupported.map(_._2)
		)
	}
}