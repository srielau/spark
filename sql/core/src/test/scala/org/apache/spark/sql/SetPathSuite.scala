/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for SET PATH command and session path management.
 * Covers the feature flag, SET PATH syntax, CURRENT_PATH() reflecting stored path,
 * DEFAULT_PATH, SYSTEM_PATH, CURRENT_SCHEMA/CURRENT_DATABASE expansion,
 * PATH (append), duplicate detection, and error conditions.
 *
 * Resolution-level tests (tables/functions resolving via the stored path)
 * belong in a separate suite once the resolution engine is wired.
 */
class SetPathSuite extends QueryTest with SharedSparkSession {

  private def withPathEnabled(f: => Unit): Unit = {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true")(f)
  }

  test("PATH disabled: CURRENT_PATH() returns default path") {
    val pathStr = sql("SELECT current_path()").collect().head.getString(0)
    assert(pathStr.contains("spark_catalog") && pathStr.contains("default"),
      s"Expected default path to contain spark_catalog.default, got: $pathStr")
  }

  test("PATH disabled: SET PATH is rejected") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("SET PATH = spark_catalog.other")
      },
      condition = "UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED",
      sqlState = "0A000",
      parameters = Map("config" -> SQLConf.PATH_ENABLED.key))
  }

  test("PATH enabled: SET PATH and CURRENT_PATH()") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.default, system.builtin")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("spark_catalog") && pathStr.contains("default"),
        s"Expected path to contain spark_catalog and default, got: $pathStr")
    }
  }

  test("PATH enabled: SET PATH = DEFAULT_PATH restores default") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.default")
      sql("SET PATH = DEFAULT_PATH")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("spark_catalog") && pathStr.contains("default"),
        s"After SET PATH = DEFAULT_PATH expected default path, got: $pathStr")
    }
  }

  test("PATH enabled: CURRENT_PATH() with DEFAULT_PATH contains current schema") {
    withPathEnabled {
      sql("SET PATH = DEFAULT_PATH")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("default"),
        s"With DEFAULT_PATH, path should contain default schema, got: $pathStr")
    }
  }

  test("direct SET of session path config is rejected") {
    val err = intercept[AnalysisException] {
      sql("SET spark.sql.session.path = 'spark_catalog.default'")
    }
    assert(err.getMessage.contains("SET PATH"),
      s"Expected SET_PATH_VIA_SET error, got: ${err.getMessage}")
  }

  test("PATH enabled: duplicate path entry raises error") {
    withPathEnabled {
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = spark_catalog.default, spark_catalog.default")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "spark_catalog.default"))
    }
  }

  test("PATH enabled: SET PATH = PATH, schema appends to path") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_append_test")
      try {
        sql("SET PATH = spark_catalog.default, system.builtin")
        sql("SET PATH = PATH, spark_catalog.path_append_test")
        val pathStr = sql("SELECT current_path()").collect().head.getString(0)
        assert(pathStr.contains("path_append_test"),
          s"PATH, schema should append; got: $pathStr")
      } finally {
        sql("DROP SCHEMA IF EXISTS path_append_test")
      }
    }
  }

  test("PATH enabled: SET PATH = CURRENT_SCHEMA / CURRENT_DATABASE") {
    withPathEnabled {
      sql("USE spark_catalog.default")
      sql("SET PATH = current_schema, system.builtin")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("spark_catalog") && pathStr.contains("default"),
        s"current_schema should expand to current schema, got: $pathStr")
      sql("SET PATH = current_database, system.builtin")
      val pathStr2 = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr2.contains("spark_catalog") && pathStr2.contains("default"),
        s"current_database should expand to current schema, got: $pathStr2")
    }
  }

  test("PATH enabled: virtual CURRENT_SCHEMA expands to USE schema") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_virt_schema")
      try {
        sql("USE spark_catalog.path_virt_schema")
        sql("SET PATH = current_schema, system.builtin")
        val pathStr = sql("SELECT current_path()").collect().head.getString(0)
        assert(pathStr.contains("path_virt_schema"),
          s"CURRENT_SCHEMA in PATH should reflect USE; got: $pathStr")
      } finally {
        sql("USE spark_catalog.default")
        sql("DROP SCHEMA IF EXISTS path_virt_schema")
      }
    }
  }

  test("PATH enabled: duplicate after expanding CURRENT_SCHEMA") {
    withPathEnabled {
      sql("USE spark_catalog.default")
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = spark_catalog.default, current_schema")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "spark_catalog.default"))
    }
  }

  test("PATH enabled: duplicate when CURRENT_SCHEMA repeated") {
    withPathEnabled {
      sql("USE spark_catalog.default")
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = current_schema, current_schema")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "spark_catalog.default"))
    }
  }

  test("PATH enabled: duplicate when SYSTEM_PATH listed twice") {
    withPathEnabled {
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = SYSTEM_PATH, SYSTEM_PATH")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "system.builtin"))
    }
  }

  test("PATH enabled: SET PATH = SYSTEM_PATH includes system.builtin and system.session") {
    withPathEnabled {
      sql("SET PATH = SYSTEM_PATH")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("builtin") && pathStr.contains("session"),
        s"SYSTEM_PATH should expand to builtin and session; got: $pathStr")
    }
  }

  test("PATH enabled: SET PATH = PATH, schema after DEFAULT_PATH (empty session path)") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_from_empty")
      try {
        sql("SET PATH = DEFAULT_PATH, system.builtin")
        sql("SET PATH = PATH, spark_catalog.path_from_empty")
        val pathStr = sql("SELECT current_path()").collect().head.getString(0)
        assert(pathStr.contains("path_from_empty"),
          s"PATH after cleared path should append schema; got: $pathStr")
      } finally {
        sql("DROP SCHEMA IF EXISTS path_from_empty")
      }
    }
  }

  test("PATH enabled: three-part schema reference is rejected") {
    withPathEnabled {
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = a.b.c")
        },
        condition = "INVALID_SQL_PATH_SCHEMA_REFERENCE",
        parameters = Map("qualifiedName" -> "a.b.c"))
    }
  }

  test("PATH enabled: stored path preserves typed case, resolution is case-insensitive") {
    withPathEnabled {
      sql("SET PATH = Spark_Catalog.Default, System.Builtin")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("Spark_Catalog") && pathStr.contains("Default"),
        s"Stored path should preserve case; got: $pathStr")
    }
  }
}
