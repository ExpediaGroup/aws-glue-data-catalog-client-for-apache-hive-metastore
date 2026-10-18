package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadOnlyRetryConditionTest {

  private final ReadOnlyRetryCondition condition = new ReadOnlyRetryCondition();

  @Test
  public void readsAreRetriedOnServerError() {
    assertTrue(condition.shouldRetry(new GetTableRequest(), serverError("InternalServiceException"), 0));
    assertTrue(condition.shouldRetry(new GetPartitionsRequest(), serverError("InternalServiceException"), 0));
    assertTrue(condition.shouldRetry(new BatchGetPartitionRequest(), serverError("InternalServiceException"), 0));
  }

  @Test
  public void readsAreRetriedOnNetworkError() {
    assertTrue(condition.shouldRetry(new GetTableRequest(), networkError(), 0));
  }

  @Test
  public void writesAreNotRetriedOnServerError() {
    // e.g. OperationTimeoutException: Glue may already have applied the write
    assertFalse(condition.shouldRetry(new UpdateTableRequest(), serverError("OperationTimeoutException"), 0));
    assertFalse(condition.shouldRetry(new CreateTableRequest(), serverError("InternalServiceException"), 0));
    assertFalse(condition.shouldRetry(new BatchCreatePartitionRequest(), serverError("InternalServiceException"), 0));
  }

  @Test
  public void writesAreNotRetriedOnNetworkError() {
    // the response can be lost after Glue applied the write
    assertFalse(condition.shouldRetry(new UpdateTableRequest(), networkError(), 0));
  }

  @Test
  public void throttledWritesAreRetried() {
    AmazonServiceException throttled = new AmazonServiceException("Rate exceeded");
    throttled.setErrorCode("ThrottlingException");
    throttled.setStatusCode(400);
    assertTrue(condition.shouldRetry(new UpdateTableRequest(), throttled, 0));
  }

  @Test
  public void unknownRequestIsNotRetried() {
    assertFalse(condition.shouldRetry(null, serverError("InternalServiceException"), 0));
  }

  private static AmazonServiceException serverError(String errorCode) {
    AmazonServiceException e = new AmazonServiceException(errorCode);
    e.setErrorCode(errorCode);
    e.setStatusCode(500);
    return e;
  }

  private static AmazonClientException networkError() {
    return new AmazonClientException("Unable to execute HTTP request", new IOException("Read timed out"));
  }
}
