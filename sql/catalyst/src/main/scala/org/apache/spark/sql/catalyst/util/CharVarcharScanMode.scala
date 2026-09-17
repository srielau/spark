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

package org.apache.spark.sql.catalyst.util

/**
 * The CHAR/VARCHAR scan mode bound to a relation (and its scan) during analysis.
 *
 * A relation carries `Option[CharVarcharScanMode]`: `None` means the relation has not been bound,
 * while a `Some` value pins the legacy, preserve-native, or standard mode so that `sameResult` /
 * cache reuse keep the variants distinct.
 */
private[sql] sealed trait CharVarcharScanMode

private[sql] object CharVarcharScanMode {
  /**
   * Use the annotated-STRING behavior that predates first-class CHAR/VARCHAR types.
   */
  case object Legacy extends CharVarcharScanMode

  /**
   * Preserve the native, constrained CHAR/VARCHAR types of the source (e.g. native ORC
   * padding/truncation). Corresponds to preserve-only semantics.
   */
  case object PreserveNative extends CharVarcharScanMode

  /**
   * Apply Spark-standard CHAR/VARCHAR semantics on read. This includes CHAR padding and
   * CHAR/VARCHAR length validation/trimming.
   */
  case object SparkStandard extends CharVarcharScanMode

  /** Returns the mode selected by the CHAR/VARCHAR SQL configurations. */
  def apply(
      firstClassTypes: Boolean,
      standardSemantics: Boolean): CharVarcharScanMode = {
    if (!firstClassTypes) {
      Legacy
    } else if (standardSemantics) {
      SparkStandard
    } else {
      PreserveNative
    }
  }

  /** Parses a mode from its `toString` name; the inverse of [[CharVarcharScanMode.toString]]. */
  def fromName(name: String): CharVarcharScanMode = name match {
    case "Legacy" => Legacy
    case "PreserveNative" => PreserveNative
    case "SparkStandard" => SparkStandard
    case other => throw new IllegalArgumentException(s"Unknown CharVarcharScanMode: $other")
  }
}
