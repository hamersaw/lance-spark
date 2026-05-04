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
package org.lance.spark;

import org.lance.index.DistanceType;
import org.lance.ipc.FullTextQuery;
import org.lance.ipc.Query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LanceSparkReadOptionsJsonTest {

  @Test
  public void testNearestJsonSerialization() {
    Query.Builder builder = new Query.Builder();
    builder.setK(10);
    builder.setColumn("vector_col");
    builder.setRefineFactor(2);
    builder.setKey(new float[] {1.0f, 2.0f, 3.0f});
    builder.setMinimumNprobes(5);
    builder.setMaximumNprobes(20);
    builder.setEf(100);
    builder.setDistanceType(DistanceType.L2);
    builder.setUseIndex(true);

    Query query = builder.build();

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").nearest(query).build();

    String json = options.getNearestJson();
    Assertions.assertNotNull(json);
    System.out.println("Serialized JSON: " + json);

    // Test deserialization via fromOptions
    Map<String, String> properties = new HashMap<>();
    properties.put("path", "s3://bucket/path");
    properties.put("nearest", json);

    LanceSparkReadOptions deserializedOptions = LanceSparkReadOptions.from(properties);
    Query deserializedQuery = deserializedOptions.getNearest();

    Assertions.assertNotNull(deserializedQuery);
    Assertions.assertEquals(query.getK(), deserializedQuery.getK());
    Assertions.assertEquals(query.getColumn(), deserializedQuery.getColumn());

    // Check RefineFactor (Optional)
    Assertions.assertTrue(deserializedQuery.getRefineFactor().isPresent());
    Assertions.assertEquals(Integer.valueOf(2), deserializedQuery.getRefineFactor().get());

    Assertions.assertArrayEquals(query.getKey(), deserializedQuery.getKey());

    // Check new fields
    Assertions.assertEquals(query.getMinimumNprobes(), deserializedQuery.getMinimumNprobes());

    Assertions.assertTrue(deserializedQuery.getMaximumNprobes().isPresent());
    Assertions.assertEquals(Integer.valueOf(20), deserializedQuery.getMaximumNprobes().get());

    Assertions.assertTrue(deserializedQuery.getEf().isPresent());
    Assertions.assertEquals(Integer.valueOf(100), deserializedQuery.getEf().get());

    Assertions.assertTrue(deserializedQuery.getDistanceType().isPresent());
    Assertions.assertEquals(DistanceType.L2, deserializedQuery.getDistanceType().get());

    Assertions.assertEquals(query.isUseIndex(), deserializedQuery.isUseIndex());
  }

  @Test
  public void testFtsEqualsAndHashCodeStabilityAcrossConstructionPaths() {
    // Two independently constructed instances carrying identical MatchQuery must be equals()
    // and have the same hashCode() — required for ReusedExchange correctness.
    FullTextQuery fts1 =
        FullTextQuery.match(
            "hello", "body", 1.5f, Optional.of(1), 50, FullTextQuery.Operator.AND, 2);
    FullTextQuery fts2 =
        FullTextQuery.match(
            "hello", "body", 1.5f, Optional.of(1), 50, FullTextQuery.Operator.AND, 2);

    LanceSparkReadOptions opts1 =
        LanceSparkReadOptions.builder().datasetUri("s3://b/p").fullTextQuery(fts1).build();
    LanceSparkReadOptions opts2 =
        LanceSparkReadOptions.builder().datasetUri("s3://b/p").fullTextQuery(fts2).build();

    Assertions.assertEquals(opts1, opts2, "Two instances with identical MatchQuery must be equal");
    Assertions.assertEquals(
        opts1.hashCode(), opts2.hashCode(), "Equal instances must have equal hashCodes");

    // Different FTS query must produce different equals result
    FullTextQuery fts3 = FullTextQuery.match("world", "body");
    LanceSparkReadOptions opts3 =
        LanceSparkReadOptions.builder().datasetUri("s3://b/p").fullTextQuery(fts3).build();
    Assertions.assertNotEquals(opts1, opts3);
  }

  @Test
  public void testNearestJsonStringInput() {
    // We use "set" prefix in Mixin configuration, but Jackson maps "k" to "setK".
    // The JSON should use property names "k", "key", "column".
    String json = "{\"column\":\"vector_col\",\"k\":10,\"refineFactor\":2,\"key\":[1.0,2.0,3.0]}";

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").nearest(json).build();

    Query query = options.getNearest();
    Assertions.assertNotNull(query);
    Assertions.assertEquals(10, query.getK());
    Assertions.assertEquals("vector_col", query.getColumn());
    Assertions.assertArrayEquals(new float[] {1.0f, 2.0f, 3.0f}, query.getKey());
  }
}
