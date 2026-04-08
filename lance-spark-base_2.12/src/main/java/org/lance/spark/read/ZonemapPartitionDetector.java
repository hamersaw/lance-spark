/*
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
package org.lance.spark.read;

import org.lance.index.scalar.ZoneStats;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Detects partition-compatible columns from zonemap statistics.
 *
 * <p>A column is "partition-compatible" if every fragment has exactly one distinct value for that
 * column — i.e., all zones within each fragment have {@code min == max} for the same value. This
 * mimics explicit partitioning (like Iceberg's identity partition) using Lance's physical data
 * layout.
 *
 * <p>When detected, the column can serve as a partition key for Spark's storage-partitioned join
 * (SPJ) protocol, enabling shuffle-free joins between Lance tables (or between Lance and
 * Iceberg/Delta tables) that share the same partition column.
 */
public final class ZonemapPartitionDetector {

  private static final Logger LOG = LoggerFactory.getLogger(ZonemapPartitionDetector.class);

  private ZonemapPartitionDetector() {}

  /**
   * Result of partition detection: the partition column name and a map from fragment ID to the
   * partition value for that fragment.
   */
  public static final class PartitionInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String columnName;
    private final Map<Integer, Comparable<?>> fragmentPartitionValues;

    public PartitionInfo(String columnName, Map<Integer, Comparable<?>> fragmentPartitionValues) {
      this.columnName = columnName;
      this.fragmentPartitionValues = Collections.unmodifiableMap(fragmentPartitionValues);
    }

    public String getColumnName() {
      return columnName;
    }

    public Map<Integer, Comparable<?>> getFragmentPartitionValues() {
      return fragmentPartitionValues;
    }

    /**
     * Returns a partition key {@link InternalRow} for the given fragment ID. The row contains a
     * single column with the partition value, converted to a Spark-compatible type.
     */
    public InternalRow partitionKeyForFragment(int fragmentId) {
      Comparable<?> value = fragmentPartitionValues.get(fragmentId);
      Object sparkValue = toSparkValue(value);
      return new GenericInternalRow(new Object[] {sparkValue});
    }

    /**
     * Converts a Java Comparable value to a Spark InternalRow compatible value (e.g., String →
     * UTF8String).
     */
    private static Object toSparkValue(Comparable<?> value) {
      if (value == null) {
        return null;
      }
      if (value instanceof String) {
        return UTF8String.fromString((String) value);
      }
      // Long, Double, Boolean, Integer are already compatible
      return value;
    }
  }

  /**
   * Detects partition-compatible columns from zonemap statistics.
   *
   * <p>Iterates over all columns with zonemap stats and checks whether each column qualifies as a
   * partition column. Returns the first qualifying column found (if any).
   *
   * @param zonemapStatsByColumn zonemap stats keyed by column name
   * @return the detected partition info, or empty if no column qualifies
   */
  public static Optional<PartitionInfo> detect(Map<String, List<ZoneStats>> zonemapStatsByColumn) {

    if (zonemapStatsByColumn == null || zonemapStatsByColumn.isEmpty()) {
      return Optional.empty();
    }

    for (Map.Entry<String, List<ZoneStats>> entry : zonemapStatsByColumn.entrySet()) {
      String column = entry.getKey();
      List<ZoneStats> zones = entry.getValue();

      Optional<Map<Integer, Comparable<?>>> partValues = extractPartitionValues(column, zones);
      if (partValues.isPresent()) {
        LOG.info(
            "Detected partition-compatible column '{}'" + " with {} fragments",
            column,
            partValues.get().size());
        return Optional.of(new PartitionInfo(column, partValues.get()));
      }
    }

    return Optional.empty();
  }

  /**
   * Checks whether a column qualifies as a partition column and extracts per-fragment partition
   * values.
   *
   * <p>A column qualifies if:
   *
   * <ul>
   *   <li>There is at least one zone
   *   <li>Every zone has non-null {@code min} and {@code max}
   *   <li>Every zone has {@code min.equals(max)} (single value)
   *   <li>All zones within the same fragment have the same value
   * </ul>
   *
   * @param column column name (for logging)
   * @param zones zonemap zones for the column
   * @return map from fragment ID to partition value, or empty if the column doesn't qualify
   */
  @SuppressWarnings("unchecked")
  static Optional<Map<Integer, Comparable<?>>> extractPartitionValues(
      String column, List<ZoneStats> zones) {

    if (zones == null || zones.isEmpty()) {
      return Optional.empty();
    }

    Map<Integer, Comparable<?>> result = new HashMap<>();

    for (ZoneStats zone : zones) {
      Comparable<?> min = zone.getMin();
      Comparable<?> max = zone.getMax();

      // Null min/max means the zone is all-null — not partition-
      // compatible (we can't assign a discrete partition value)
      if (min == null || max == null) {
        LOG.debug(
            "Column '{}': fragment {} has null min/max," + " not partition-compatible",
            column,
            zone.getFragmentId());
        return Optional.empty();
      }

      // Check min == max (single distinct value in this zone)
      if (!min.equals(max)) {
        LOG.debug(
            "Column '{}': fragment {} zone has min={} != max={}," + " not partition-compatible",
            column,
            zone.getFragmentId(),
            min,
            max);
        return Optional.empty();
      }

      // Check consistency within the same fragment
      int fragId = zone.getFragmentId();
      Comparable<?> existing = result.get(fragId);
      if (existing != null && !existing.equals(min)) {
        LOG.debug(
            "Column '{}': fragment {} has multiple values"
                + " ({} and {}), not partition-compatible",
            column,
            fragId,
            existing,
            min);
        return Optional.empty();
      }

      result.put(fragId, min);
    }

    return Optional.of(result);
  }
}
