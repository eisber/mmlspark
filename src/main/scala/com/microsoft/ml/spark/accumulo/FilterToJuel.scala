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

	def serializeFilter(filter: Filter, attributeToVariableMapping: Map[String, String]): String = {
		val m = attributeToVariableMapping
		filter match {
			case op: And => s"(${serializeFilter(op.left, m)} && ${serializeFilter(op.right, m)})"
			case op: Or => s"(${serializeFilter(op.left, m)} || ${serializeFilter(op.right, m)})"
			case op: EqualTo =>  s"(${m.get(op.attribute).get} == ${serializeValue(op.value)})"
			case op: GreaterThan => s"(${m.get(op.attribute).get} > ${serializeValue(op.value)})"
			case op: GreaterThanOrEqual => s"(${m.get(op.attribute).get} >= ${serializeValue(op.value)})"
			case op: LessThan => s"(${m.get(op.attribute).get} < ${serializeValue(op.value)})"
			case op: LessThanOrEqual => s"(${m.get(op.attribute).get} <= ${serializeValue(op.value)})"
			case op: Not => s"(!${serializeFilter(op.child, m)})"
			case op: IsNull => s"(${m.get(op.attribute).get} == null)"
			case op: IsNotNull => s"(${m.get(op.attribute).get} != null)"
			case op: StringContains => s"${m.get(op.attribute).get}.contains(${serializeValue(op.value)})"
			case op: StringStartsWith => s"${m.get(op.attribute).get}.startsWith(${serializeValue(op.value)})"
			case op: StringEndsWith => s"${m.get(op.attribute).get}.endsWith(${serializeValue(op.value)})"
			case op: In => {
				val values = op.values.map { v => serializeValue(v) } .mkString(",")
				s"${m.get(op.attribute).get}.in(${values})"
			}
			// TODO: not sure if null handling is properly done
			// TODO:  EqualNullSafe
			case _ => throw new UnsupportedOperationException(s"Filter ${filter} not supported")
		}
	}

	def serializeFilters(filters: Array[Filter], attributeToVariableMapping: Map[String, String]): AccumuloFilterResult =
	{
		val (supported, unsupported) = filters.map({ f => {

			try {
				(serializeFilter(f, attributeToVariableMapping), f)
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
