// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.spark.sql.sources._

case class AccumuloFilterResult(val serializedFilter: String,
								val supportedFilters: Seq[Filter],
								val unsupportedFilters: Seq[Filter])

class FilterToJuel(val attributeToVariableMapping: Map[String, String], val rowKeyColumn: String = "rowKey") {
	def mapAttribute(attribute: String): String = {
		if (attribute == rowKeyColumn) {
			"rowKey"
		}
		else {
			val opt = attributeToVariableMapping.get(attribute)

			if (opt.isEmpty) {
				println(s"UNABLE TO MAP attribute ${attribute}")
			}

			opt.get
		}
	}

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
			case op: EqualTo =>  s"(${mapAttribute(op.attribute)} == ${serializeValue(op.value)})"
			case op: GreaterThan => s"(${mapAttribute(op.attribute)} > ${serializeValue(op.value)})"
			case op: GreaterThanOrEqual => s"(${mapAttribute(op.attribute)} >= ${serializeValue(op.value)})"
			case op: LessThan => s"(${mapAttribute(op.attribute)} < ${serializeValue(op.value)})"
			case op: LessThanOrEqual => s"(${mapAttribute(op.attribute)} <= ${serializeValue(op.value)})"
			case op: Not => s"(!${serializeFilter(op.child)})"
			case op: IsNull => s"(${mapAttribute(op.attribute)} == null)"
			case op: IsNotNull => {
				// IsNotNull(cf1) will be generated for conditions like cf1.cq1 > 5
				// since we always create the struct, it's always true
				val variable = attributeToVariableMapping.get(op.attribute)

				if (variable.isEmpty)
					// assuming this comes for a nested column family, will always be true
					"true"
				else
					s"(${variable.get} != null)"
			}
			case op: StringContains => s"${mapAttribute(op.attribute)}.contains(${serializeValue(op.value)})"
			case op: StringStartsWith => s"${mapAttribute(op.attribute)}.startsWith(${serializeValue(op.value)})"
			case op: StringEndsWith => s"${mapAttribute(op.attribute)}.endsWith(${serializeValue(op.value)})"
			case op: In => {
				val values = op.values.map { v => serializeValue(v) } .mkString(",")
				s"${mapAttribute(op.attribute)}.in(${values})"
			}
			// TODO: not sure if null handling is properly done
			// TODO:  EqualNullSafe
			case _ => throw new UnsupportedOperationException(s"Filter ${filter} not supported")
		}
	}

	def serializeFilters(filters: Array[Filter], filterStr: String): AccumuloFilterResult =
	{
		val (supported, unsupported) = filters.map({ f => {

			try {
				(serializeFilter(f), f)
			} catch {
				case e: UnsupportedOperationException => ("", f)
			}
		}}).partition(!_._1.isEmpty)

		var filter = supported.map(_._1)

		// append if provided
		if (filterStr.length > 0)
			filter :+ filterStr

		val finalFilter = filter.mkString(" && ")

		AccumuloFilterResult(
			finalFilter,
			supported.map(_._2),
			unsupported.map(_._2)
		)
	}
}
