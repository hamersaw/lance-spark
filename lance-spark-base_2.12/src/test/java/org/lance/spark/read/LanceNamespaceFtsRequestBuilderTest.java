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

import org.lance.ipc.FullTextQuery;
import org.lance.namespace.model.MatchQuery;
import org.lance.namespace.model.QueryTableRequest;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LanceNamespaceFtsRequestBuilderTest {

  private static final List<String> TABLE_ID = Arrays.asList("db", "docs");

  private static MatchQuery match(QueryTableRequest req) {
    return req.getFullTextQuery().getStructuredQuery().getQuery().getMatch();
  }

  @Test
  public void testOmitsKWhenNull() {
    QueryTableRequest req =
        LanceNamespaceFtsRequestBuilder.build(
            TABLE_ID, FullTextQuery.match("q", "body"), null, null, null, null, null, null);
    // no LIMIT -> k omitted so the namespace returns all matching rows
    assertNull(req.getK());
    assertEquals(TABLE_ID, req.getId());
    assertEquals("q", match(req).getTerms());
    assertEquals("body", match(req).getColumn());
  }

  @Test
  public void testSetsAllFields() {
    QueryTableRequest req =
        LanceNamespaceFtsRequestBuilder.build(
            TABLE_ID,
            FullTextQuery.match("q", "body"),
            Arrays.asList("id", "body", "_score"),
            "year >= 2024",
            10,
            5,
            3L,
            Boolean.TRUE);
    assertEquals(10, req.getK().intValue());
    assertEquals("year >= 2024", req.getFilter());
    assertEquals(Arrays.asList("id", "body", "_score"), req.getColumns().getColumnNames());
    assertEquals(5, req.getOffset().intValue());
    assertEquals(3L, req.getVersion().longValue());
    assertEquals(Boolean.TRUE, req.getWithRowId());
  }

  @Test
  public void testOmitsColumnsWhenEmpty() {
    QueryTableRequest req =
        LanceNamespaceFtsRequestBuilder.build(
            TABLE_ID,
            FullTextQuery.match("q", "body"),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            null);
    assertNull(req.getColumns());
  }
}
