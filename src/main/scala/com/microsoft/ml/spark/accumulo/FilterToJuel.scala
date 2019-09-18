// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo


import org.apache.spark.sql.sources._

object FilterToJuel {

	def serializeFilter(filter: Filter): String = {
		"" 
	}

	def serializeFilters(filters: Array[Filter]): String = {
		""
	}

}