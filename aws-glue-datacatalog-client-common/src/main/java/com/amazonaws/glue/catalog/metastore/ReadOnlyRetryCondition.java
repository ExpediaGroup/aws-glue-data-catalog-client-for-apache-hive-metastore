package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.retry.RetryUtils;

/**
 * Lets the AWS SDK retry only Glue calls that are safe to send twice.
 * <p>
 * Writes such as UpdateTable carry a VersionId and are not idempotent. When an attempt fails ambiguously
 * (e.g. Glue returns OperationTimeoutException, or the response is lost) Glue may already have applied it,
 * and a retry is then rejected with ConcurrentModificationException. That reaches Iceberg as a failed
 * commit, so Iceberg deletes the new metadata file the table already points to. Without the retry the
 * original error reaches the client, which can check the outcome itself (Iceberg reloads the table).
 * <p>
 * Reads are retried as before. Throttled writes are also retried: Glue rejected them, so they were not applied.
 * Same approach as Waggle Dance only retrying get calls on its Thrift clients.
 */
public final class ReadOnlyRetryCondition implements RetryPolicy.RetryCondition {

  private static final String[] READ_REQUEST_PREFIXES = {"Get", "BatchGet", "Search"};

  private final RetryPolicy.RetryCondition delegate;

  public ReadOnlyRetryCondition() {
    this(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION);
  }

  ReadOnlyRetryCondition(RetryPolicy.RetryCondition delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean shouldRetry(AmazonWebServiceRequest originalRequest, AmazonClientException exception,
      int retriesAttempted) {
    if (!isRead(originalRequest) && !RetryUtils.isThrottlingException(exception)) {
      return false;
    }
    return delegate.shouldRetry(originalRequest, exception, retriesAttempted);
  }

  private static boolean isRead(AmazonWebServiceRequest request) {
    if (request == null) {
      return false;
    }
    String name = request.getClass().getSimpleName();
    for (String prefix : READ_REQUEST_PREFIXES) {
      if (name.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }
}
