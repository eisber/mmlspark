// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import java.io.ByteArrayOutputStream

import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.sql.sources._

class VerifyFilterToJuel extends TestBase {
  val map = Map[String, String](
    "i" -> "i0",
    "x" -> "x",
    "j" -> "j",
    "k" -> "k",
    "x.yZ" -> "xyZ")

  test("Validate filter to juel operators") {
    assert("(i0 == 5)".equals(FilterToJuel.serializeFilter(new EqualTo("i", 5), map)))
    assert("(i0 >= 5)".equals(FilterToJuel.serializeFilter(new GreaterThanOrEqual("i", 5), map)))
    assert("(i0 > 5)".equals(FilterToJuel.serializeFilter(new GreaterThan("i", 5), map)))
    assert("(i0 <= 5)".equals(FilterToJuel.serializeFilter(new LessThanOrEqual("i", 5), map)))
    assert("(i0 < 5)".equals(FilterToJuel.serializeFilter(new LessThan("i", 5), map)))
    assert("(i0 == null)".equals(FilterToJuel.serializeFilter(new IsNull("i"), map)))
    assert("(i0 != null)".equals(FilterToJuel.serializeFilter(new IsNotNull("i"), map)))
  }

  test("Validate filter to juel composed operators") {
    assert("(!(i0 == 5))".equals(FilterToJuel.serializeFilter(
      new Not(new EqualTo("i", 5)), map)))

    assert("((i0 == 5) && (x == 3.0))".equals(FilterToJuel.serializeFilter(
      new And(new EqualTo("i", 5), new EqualTo("x", 3.0)), map)))

    assert("((i0 == 5) || (x == 3.0))".equals(FilterToJuel.serializeFilter(
      new Or(new EqualTo("i", 5), new EqualTo("x", 3.0)), map)))
  }

  test("Validate filter to juel string operators") {
    assert("x.contains('abc')".equals(FilterToJuel.serializeFilter(
      new StringContains("x", "abc"), map)))
    assert("x.startsWith('abc')".equals(FilterToJuel.serializeFilter(
      new StringStartsWith("x", "abc"), map)))
    assert("x.endsWith('abc')".equals(FilterToJuel.serializeFilter(
      new StringEndsWith("x", "abc"), map)))
  }

  test("Validate filter to juel in operator") {
    assert("xyZ.in('abc','def','ghi')".equals(FilterToJuel.serializeFilter(
      new In("x.yZ", Array("abc", "def", "ghi")), map)))
  }

  test("Validate filter string escape") {
    assert("(i0 == '\\'')".equals(FilterToJuel.serializeFilter(new EqualTo("i", "'"), map)))
    assert("(i0 == '\\\\')".equals(FilterToJuel.serializeFilter(new EqualTo("i", "\\"), map)))
    assert("(i0 == '\\\\\\'')".equals(FilterToJuel.serializeFilter(new EqualTo("i", "\\'"), map)))
  }

  test("Validate filter combining") {
    val filters = Array[Filter](
      new EqualTo("i", 5),
      new EqualTo("j", 3),
      new EqualTo("k", 4)
    )

    val result = FilterToJuel.serializeFilters(filters, map)

    assert("${(i0 == 5) && (j == 3) && (k == 4)}".equals(result.serializedFilter))
    assert(filters.length == result.supportedFilters.length)

    assert(result.unsupportedFilters.isEmpty)

  }
}
