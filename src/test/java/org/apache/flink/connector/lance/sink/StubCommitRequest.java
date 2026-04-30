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
package org.apache.flink.connector.lance.sink;

import org.apache.flink.api.connector.sink2.Committer;

/**
 * Minimal {@link Committer.CommitRequest} stand-in for unit tests that exercise a {@link Committer}
 * directly without spinning up Flink's runtime. Only {@code getCommittable} is meaningful; the
 * retry-signalling methods are no-ops.
 */
final class StubCommitRequest<T> implements Committer.CommitRequest<T> {

  private final T committable;

  StubCommitRequest(T committable) {
    this.committable = committable;
  }

  @Override
  public T getCommittable() {
    return committable;
  }

  @Override
  public int getNumberOfRetries() {
    return 0;
  }

  @Override
  public void signalFailedWithKnownReason(Throwable t) {}

  @Override
  public void signalFailedWithUnknownReason(Throwable t) {}

  @Override
  public void retryLater() {}

  @Override
  public void updateAndRetryLater(T committable) {}

  @Override
  public void signalAlreadyCommitted() {}
}
