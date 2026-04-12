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
package org.lance.spark.write;

import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;

import com.google.common.base.Preconditions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Buffers Spark rows into Arrow batches for consumption by Lance fragment creation.
 *
 * <p>This class bridges the producer (Spark thread writing rows) and consumer (Lance native code
 * pulling batches via ArrowReader interface). It uses a lock with conditions to synchronize between
 * the two threads - the producer blocks until the consumer is ready for more data, and vice versa.
 *
 * <p>Batches are flushed when either the row count reaches {@code batchSize} or the total allocator
 * memory exceeds {@code maxBatchBytes}, whichever comes first. This prevents OOM when individual
 * rows are very large (e.g., rows with large binary/string columns or embeddings).
 *
 * <p>Note: because this buffer reuses a single VectorSchemaRoot across batches, the allocator
 * retains capacity from previous batches. The byte-based flush triggers when the allocator's total
 * memory exceeds the threshold, which is conservative but safe — it bounds total memory regardless
 * of how Arrow internally manages buffer capacity.
 *
 * @see QueuedArrowBatchWriteBuffer for a queue-based alternative with per-batch allocators
 */
public class SemaphoreArrowBatchWriteBuffer extends ArrowBatchWriteBuffer {
  private final Schema schema;
  private final StructType sparkSchema;
  private final int batchSize;
  private final long maxBatchBytes;

  /**
   * Child allocator used to track memory for this buffer. Since the parent allocator may be shared
   * globally, we use a child allocator so that {@code getAllocatedMemory()} reflects only this
   * buffer's memory.
   */
  private final BufferAllocator batchAllocator;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition canWrite = lock.newCondition();
  private final Condition batchReady = lock.newCondition();

  @GuardedBy("lock")
  private boolean finished = false;

  @GuardedBy("lock")
  private int count;

  private org.lance.spark.arrow.LanceArrowWriter arrowWriter = null;

  public SemaphoreArrowBatchWriteBuffer(
      BufferAllocator allocator,
      Schema schema,
      StructType sparkSchema,
      int batchSize,
      long maxBatchBytes) {
    // Pass a child allocator to ArrowReader so VectorSchemaRoot allocation is tracked
    super(allocator.newChildAllocator("semaphore-buffer", 0, Long.MAX_VALUE));
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(batchSize > 0);
    Preconditions.checkArgument(maxBatchBytes > 0, "maxBatchBytes must be positive");
    this.schema = schema;
    this.sparkSchema = sparkSchema;
    this.batchSize = batchSize;
    this.maxBatchBytes = maxBatchBytes;
    this.batchAllocator = this.allocator;
    // Start with count = batchSize so the writer blocks on canWrite.await() until the
    // reader's prepareLoadNextBatch() initializes arrowWriter and resets count to 0.
    this.count = batchSize;
  }

  public SemaphoreArrowBatchWriteBuffer(
      BufferAllocator allocator, Schema schema, StructType sparkSchema, int batchSize) {
    this(allocator, schema, sparkSchema, batchSize, LanceSparkWriteOptions.DEFAULT_MAX_BATCH_BYTES);
  }

  /** Simplified constructor that uses LanceRuntime allocator and converts Spark schema to Arrow. */
  public SemaphoreArrowBatchWriteBuffer(StructType sparkSchema, int batchSize) {
    this(sparkSchema, batchSize, false);
  }

  /** Simplified constructor with large var types support. */
  public SemaphoreArrowBatchWriteBuffer(
      StructType sparkSchema, int batchSize, boolean useLargeVarTypes) {
    this(sparkSchema, batchSize, useLargeVarTypes, LanceSparkWriteOptions.DEFAULT_MAX_BATCH_BYTES);
  }

  /** Simplified constructor that uses LanceRuntime allocator and converts Spark schema to Arrow. */
  public SemaphoreArrowBatchWriteBuffer(StructType sparkSchema, int batchSize, long maxBatchBytes) {
    this(sparkSchema, batchSize, false, maxBatchBytes);
  }

  /** Simplified constructor with large var types and maxBatchBytes support. */
  public SemaphoreArrowBatchWriteBuffer(
      StructType sparkSchema, int batchSize, boolean useLargeVarTypes, long maxBatchBytes) {
    this(
        LanceRuntime.allocator(),
        LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false, useLargeVarTypes),
        sparkSchema,
        batchSize,
        maxBatchBytes);
  }

  @Override
  public void onTaskComplete() {
    lock.lock();
    try {
      canWrite.signalAll();
      batchReady.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /** Returns whether the current batch should be flushed based on byte size. */
  private boolean isBatchFullByBytes() {
    if (maxBatchBytes == Long.MAX_VALUE) {
      return false;
    }
    return batchAllocator.getAllocatedMemory() >= maxBatchBytes;
  }

  /** Returns whether the current batch should be flushed (by row count or byte size). */
  private boolean isBatchFull() {
    return count >= batchSize || (count > 0 && isBatchFullByBytes());
  }

  @Override
  public void write(InternalRow row) {
    Preconditions.checkNotNull(row);
    lock.lock();
    try {
      checkForError();

      // wait until prepareLoadNextBatch signals that writes are available
      while (isBatchFull()) {
        canWrite.await();
        checkForError();
      }

      arrowWriter.write(row);
      count++;

      if (isBatchFull()) {
        batchReady.signal();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void setFinished() {
    lock.lock();
    try {
      finished = true;
      batchReady.signal();
      canWrite.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void prepareLoadNextBatch() throws IOException {
    super.prepareLoadNextBatch();
    arrowWriter =
        org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(
            this.getVectorSchemaRoot(), sparkSchema);
    lock.lock();
    try {
      count = 0;
      canWrite.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    prepareLoadNextBatch();
    lock.lock();
    try {
      if (finished && count == 0) {
        return false;
      }

      // wait until batch is full (by rows or bytes) or finished
      while (!isBatchFull() && !finished) {
        batchReady.await();
        checkForError();
      }

      arrowWriter.finish();

      if (!finished) {
        return true;
      } else {
        return count > 0;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long bytesRead() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void closeReadSource() throws IOException {
    // Close the child allocator that was created for byte tracking.
    // The VectorSchemaRoot is closed by ArrowReader.close() before this is called.
    batchAllocator.close();
  }

  @Override
  protected Schema readSchema() {
    return this.schema;
  }
}
