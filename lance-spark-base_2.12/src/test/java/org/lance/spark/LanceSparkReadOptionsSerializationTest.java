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

import org.lance.ipc.FullTextQuery;
import org.lance.ipc.Query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LanceSparkReadOptionsSerializationTest {

  @Test
  public void testJavaSerialization() throws IOException, ClassNotFoundException {
    String json = "{\"column\":\"vector_col\",\"k\":10,\"key\":[1.0,2.0,3.0]}";

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").nearest(json).build();

    Query originalQuery = options.getNearest();
    Assertions.assertNotNull(originalQuery);

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(options);
    oos.close();

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    LanceSparkReadOptions deserializedOptions = (LanceSparkReadOptions) ois.readObject();

    Query deserializedQuery = deserializedOptions.getNearest();

    Assertions.assertNotNull(
        deserializedQuery, "Nearest query should not be null after deserialization");
    Assertions.assertEquals(originalQuery.getK(), deserializedQuery.getK());
    Assertions.assertEquals(originalQuery.getColumn(), deserializedQuery.getColumn());
    Assertions.assertArrayEquals(originalQuery.getKey(), deserializedQuery.getKey());
  }

  @Test
  public void testFullTextQuerySerializationRoundTrip() throws IOException, ClassNotFoundException {
    FullTextQuery fts =
        FullTextQuery.match(
            "search term", "body", 1.0f, Optional.of(1), 50, FullTextQuery.Operator.OR, 0);

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").fullTextQuery(fts).build();

    Assertions.assertNotNull(options.getFullTextQuery());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(options);
    oos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    LanceSparkReadOptions deserialized = (LanceSparkReadOptions) ois.readObject();

    Assertions.assertNotNull(
        deserialized.getFullTextQuery(), "fullTextQuery should not be null after deserialization");
    FullTextQuery rtFts = deserialized.getFullTextQuery();
    Assertions.assertEquals(FullTextQuery.Type.MATCH, rtFts.getType());
    FullTextQuery.MatchQuery mq = (FullTextQuery.MatchQuery) rtFts;
    Assertions.assertEquals("search term", mq.getQueryText());
    Assertions.assertEquals("body", mq.getColumn());
  }

  @Test
  public void testWithVersionPropagatesFullTextQuery() {
    FullTextQuery fts = FullTextQuery.phrase("quick brown fox", "body", 1);
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").fullTextQuery(fts).build();

    LanceSparkReadOptions versioned = options.withVersion(42);
    Assertions.assertNotNull(
        versioned.getFullTextQuery(), "fullTextQuery must not be dropped by withVersion()");
    Assertions.assertEquals(42, versioned.getVersion());
    // Structural equality check
    Assertions.assertTrue(
        org.lance.spark.utils.FullTextQueryUtils.equals(
            options.getFullTextQuery(), versioned.getFullTextQuery()));
  }

  @Test
  public void testFromOptionsParsesFtsSubtype() {
    FullTextQuery original = FullTextQuery.phrase("hello world", "body", 0);
    String json = org.lance.spark.utils.FullTextQueryUtils.fullTextQueryToString(original);

    Map<String, String> props = new HashMap<>();
    props.put("path", "s3://bucket/path");
    props.put(LanceSparkReadOptions.CONFIG_FULL_TEXT_QUERY, json);

    LanceSparkReadOptions opts = LanceSparkReadOptions.from(props);
    Assertions.assertNotNull(opts.getFullTextQuery());
    Assertions.assertEquals(FullTextQuery.Type.MATCH_PHRASE, opts.getFullTextQuery().getType());
    FullTextQuery.PhraseQuery pq = (FullTextQuery.PhraseQuery) opts.getFullTextQuery();
    Assertions.assertEquals("hello world", pq.getQueryText());
    Assertions.assertEquals("body", pq.getColumn());
  }

  @Test
  public void testMultiMatchQuerySerializationRoundTrip()
      throws IOException, ClassNotFoundException {
    FullTextQuery fts = FullTextQuery.multiMatch("hello world", Arrays.asList("body", "title"));

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").fullTextQuery(fts).build();

    Assertions.assertNotNull(options.getFullTextQuery());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(options);
    oos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    LanceSparkReadOptions deserialized = (LanceSparkReadOptions) ois.readObject();

    Assertions.assertNotNull(
        deserialized.getFullTextQuery(), "fullTextQuery must not be null after deserialization");
    Assertions.assertEquals(
        FullTextQuery.Type.MULTI_MATCH, deserialized.getFullTextQuery().getType());
    FullTextQuery.MultiMatchQuery mmq =
        (FullTextQuery.MultiMatchQuery) deserialized.getFullTextQuery();
    Assertions.assertEquals("hello world", mmq.getQueryText());
    Assertions.assertEquals(Arrays.asList("body", "title"), mmq.getColumns());
    Assertions.assertEquals(FullTextQuery.Operator.OR, mmq.getOperator());
    Assertions.assertFalse(
        mmq.getBoosts().isPresent(), "boosts must be absent for default multiMatch");
  }

  @Test
  public void testWithVersionPropagatesMultiMatchQuery() {
    FullTextQuery fts = FullTextQuery.multiMatch("quick fox", Arrays.asList("title", "body"));
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").fullTextQuery(fts).build();

    LanceSparkReadOptions versioned = options.withVersion(7);
    Assertions.assertNotNull(
        versioned.getFullTextQuery(), "fullTextQuery must not be dropped by withVersion()");
    Assertions.assertEquals(7, versioned.getVersion());
    Assertions.assertTrue(
        org.lance.spark.utils.FullTextQueryUtils.equals(
            options.getFullTextQuery(), versioned.getFullTextQuery()));
  }

  @Test
  public void testFromOptionsParsesFtsMultiMatchSubtype() {
    FullTextQuery original = FullTextQuery.multiMatch("search term", Arrays.asList("col1", "col2"));
    String json = org.lance.spark.utils.FullTextQueryUtils.fullTextQueryToString(original);

    Map<String, String> props = new HashMap<>();
    props.put("path", "s3://bucket/path");
    props.put(LanceSparkReadOptions.CONFIG_FULL_TEXT_QUERY, json);

    LanceSparkReadOptions opts = LanceSparkReadOptions.from(props);
    Assertions.assertNotNull(opts.getFullTextQuery());
    Assertions.assertEquals(FullTextQuery.Type.MULTI_MATCH, opts.getFullTextQuery().getType());
    FullTextQuery.MultiMatchQuery mmq = (FullTextQuery.MultiMatchQuery) opts.getFullTextQuery();
    Assertions.assertEquals("search term", mmq.getQueryText());
    Assertions.assertEquals(Arrays.asList("col1", "col2"), mmq.getColumns());
    Assertions.assertEquals(FullTextQuery.Operator.OR, mmq.getOperator());
  }

  @Test
  public void testUseIndexSerialization() throws IOException, ClassNotFoundException {
    // Case 1: useIndex is explicitly set to false
    String jsonFalse =
        "{\"column\":\"vector_col\",\"k\":10,\"key\":[1.0,2.0,3.0],\"useIndex\":false}";
    LanceSparkReadOptions optionsFalse =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").nearest(jsonFalse).build();

    Query queryFalse = optionsFalse.getNearest();
    Assertions.assertFalse(queryFalse.isUseIndex());

    // Serialize
    ByteArrayOutputStream baosFalse = new ByteArrayOutputStream();
    ObjectOutputStream oosFalse = new ObjectOutputStream(baosFalse);
    oosFalse.writeObject(optionsFalse);
    oosFalse.close();

    // Deserialize
    ByteArrayInputStream baisFalse = new ByteArrayInputStream(baosFalse.toByteArray());
    ObjectInputStream oisFalse = new ObjectInputStream(baisFalse);
    LanceSparkReadOptions deserializedOptionsFalse = (LanceSparkReadOptions) oisFalse.readObject();

    Assertions.assertFalse(
        deserializedOptionsFalse.getNearest().isUseIndex(),
        "useIndex should remain false after serialization/deserialization");

    // Case 2: useIndex is explicitly set to true
    String jsonTrue =
        "{\"column\":\"vector_col\",\"k\":10,\"key\":[1.0,2.0,3.0],\"useIndex\":true}";
    LanceSparkReadOptions optionsTrue =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").nearest(jsonTrue).build();

    Query queryTrue = optionsTrue.getNearest();
    Assertions.assertTrue(queryTrue.isUseIndex());

    // Serialize
    ByteArrayOutputStream baosTrue = new ByteArrayOutputStream();
    ObjectOutputStream oosTrue = new ObjectOutputStream(baosTrue);
    oosTrue.writeObject(optionsTrue);
    oosTrue.close();

    // Deserialize
    ByteArrayInputStream baisTrue = new ByteArrayInputStream(baosTrue.toByteArray());
    ObjectInputStream oisTrue = new ObjectInputStream(baisTrue);
    LanceSparkReadOptions deserializedOptionsTrue = (LanceSparkReadOptions) oisTrue.readObject();

    Assertions.assertTrue(
        deserializedOptionsTrue.getNearest().isUseIndex(),
        "useIndex should remain true after serialization/deserialization");
  }

  @Test
  public void testExecutorCredentialRefreshDefaultsToTrue() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").build();
    Assertions.assertTrue(
        options.isExecutorCredentialRefresh(),
        "executor_credential_refresh must default to true to preserve existing behavior");
  }

  @Test
  public void testExecutorCredentialRefreshParsedFromOptions() {
    LanceSparkReadOptions optionsFalse =
        LanceSparkReadOptions.from(
            Collections.singletonMap(
                LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "false"),
            "s3://bucket/path");
    Assertions.assertFalse(optionsFalse.isExecutorCredentialRefresh());

    LanceSparkReadOptions optionsTrue =
        LanceSparkReadOptions.from(
            Collections.singletonMap(
                LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "true"),
            "s3://bucket/path");
    Assertions.assertTrue(optionsTrue.isExecutorCredentialRefresh());
  }

  @Test
  public void testExecutorCredentialRefreshSurvivesSerialization()
      throws IOException, ClassNotFoundException {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .executorCredentialRefresh(false)
            .build();
    Assertions.assertFalse(options.isExecutorCredentialRefresh());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(options);
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    LanceSparkReadOptions deserialized;
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserialized = (LanceSparkReadOptions) ois.readObject();
    }

    Assertions.assertFalse(
        deserialized.isExecutorCredentialRefresh(),
        "executor_credential_refresh must survive Java serialization (driver -> executor handoff)");
  }

  @Test
  public void testExecutorCredentialRefreshPreservedByWithVersion() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .executorCredentialRefresh(false)
            .build();

    LanceSparkReadOptions pinned = options.withVersion(7);
    Assertions.assertFalse(
        pinned.isExecutorCredentialRefresh(),
        "withVersion() must propagate the executor_credential_refresh flag");
  }

  /**
   * Catalog-level config (set via {@code --conf spark.sql.catalog.<name>.<key>}) is the only route
   * available to SQL DML (DELETE / UPDATE / MERGE INTO), which has no per-statement {@code
   * .option(...)} attach point. This test guards the catalog-conf path.
   */
  @Test
  public void testExecutorCredentialRefreshFromCatalogDefaults() {
    Map<String, String> catalogOpts = new HashMap<>();
    catalogOpts.put(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "false");
    LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOpts);

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .withCatalogDefaults(catalogConfig)
            .build();

    Assertions.assertFalse(
        options.isExecutorCredentialRefresh(),
        "executor_credential_refresh set at catalog level must land in the typed field "
            + "so it takes effect for SELECT without .option(...) and for SQL DML");
  }

  /**
   * Spark's scan-time options (via {@code spark.read.option(...)}) go through a second {@code
   * fromOptions(mergedMap)} rebuild in {@code LanceDataset.newScanBuilder}. Per-read settings must
   * win over catalog-level defaults.
   */
  @Test
  public void testPerReadOptionOverridesCatalogDefaults() {
    Map<String, String> catalogOpts = new HashMap<>();
    catalogOpts.put(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "false");
    LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOpts);

    // Simulate the rebuild path in LanceDataset.newScanBuilder: the builder starts by applying
    // the catalog defaults, then fromOptions() replays against the merged (catalog + per-read)
    // map where the per-read value wins.
    Map<String, String> merged = new HashMap<>(catalogConfig.getStorageOptions());
    merged.put(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "true");

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .withCatalogDefaults(catalogConfig)
            .fromOptions(merged)
            .build();

    Assertions.assertTrue(
        options.isExecutorCredentialRefresh(),
        "per-read .option(...) must override the catalog-level default");
  }
}
