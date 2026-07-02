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
import org.lance.namespace.model.QueryTableRequest;
import org.lance.namespace.model.QueryTableRequestColumns;
import org.lance.namespace.model.QueryTableRequestFullTextQuery;
import org.lance.namespace.model.QueryTableRequestVector;
import org.lance.spark.utils.FullTextQueryConverter;

import java.util.List;

/**
 * Builds a namespace {@link QueryTableRequest} for a full-text search from the shared scan spec.
 * Used by the namespace-backend dispatch when a namespace is configured; the canonical {@link
 * FullTextQuery} is carried as the structured namespace model via {@link FullTextQueryConverter}.
 *
 * <p>{@code k} is <b>omitted when null</b> so the namespace returns every matching row — the bare
 * {@code WHERE lance_match(...)} shape with no {@code LIMIT}. Callers pass a bounded {@code k} only
 * for ranked / limited queries, and only when it is safe to push (no residual predicate above the
 * scan).
 */
public final class LanceNamespaceFtsRequestBuilder {

  private LanceNamespaceFtsRequestBuilder() {}

  public static QueryTableRequest build(
      List<String> tableId,
      FullTextQuery fullTextQuery,
      List<String> columns,
      String filter,
      Integer k,
      Integer offset,
      Long version,
      Boolean withRowId) {
    QueryTableRequest request = new QueryTableRequest().id(tableId);
    // queryTable expects the vector object present even for a pure full-text query.
    request.vector(new QueryTableRequestVector());
    request.fullTextQuery(
        new QueryTableRequestFullTextQuery()
            .structuredQuery(FullTextQueryConverter.toStructuredFtsQuery(fullTextQuery)));
    if (k != null) {
      request.k(k);
    }
    if (columns != null && !columns.isEmpty()) {
      request.columns(new QueryTableRequestColumns().columnNames(columns));
    }
    if (filter != null) {
      request.filter(filter);
    }
    if (offset != null) {
      request.offset(offset);
    }
    if (version != null) {
      request.version(version);
    }
    if (withRowId != null) {
      request.withRowId(withRowId);
    }
    return request;
  }
}
