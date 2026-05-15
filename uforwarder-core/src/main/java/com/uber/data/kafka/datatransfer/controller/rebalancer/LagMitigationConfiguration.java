package com.uber.data.kafka.datatransfer.controller.rebalancer;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the consumer-lag mitigation feature.
 *
 * <p>When a partition's consumer lag (brokerEndOffset - committedOffset) exceeds {@link
 * #lagOffsetThreshold}, the {@link LagMitigationRebalancer} will:
 *
 * <ol>
 *   <li>Rewind the main consumer to the latest offset so fresh messages are delivered immediately.
 *   <li>Spin up a bounded catch-up consumer that replays the lagging range using a separate
 *       consumer group.
 * </ol>
 *
 * <p>Properties are bound under the prefix {@code master.lag.mitigation}.
 */
@ConfigurationProperties(prefix = "master.lag.mitigation")
public class LagMitigationConfiguration {

  /** Whether the lag mitigation feature is active. Defaults to false (safe default). */
  private boolean enabled = false;

  /**
   * Number of messages behind the broker end offset that constitutes "lagging". Only partitions
   * whose lag exceeds this threshold trigger catch-up consumer creation.
   */
  private long lagOffsetThreshold = 100_000L;

  /**
   * Maximum number of catch-up consumer jobs that may be active at the same time across all job
   * groups. Prevents unbounded resource consumption during mass-lag events.
   */
  private int maxConcurrentCatchUpJobs = 10;

  /**
   * Suffix appended to the original consumer group name to form the catch-up consumer group name.
   * E.g. with suffix "-catchup", group "my-group" becomes "my-group-catchup".
   */
  private String catchUpConsumerGroupSuffix = "-catchup";

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public long getLagOffsetThreshold() {
    return lagOffsetThreshold;
  }

  public void setLagOffsetThreshold(long lagOffsetThreshold) {
    if (lagOffsetThreshold <= 0) {
      throw new IllegalArgumentException("lagOffsetThreshold must be > 0");
    }
    this.lagOffsetThreshold = lagOffsetThreshold;
  }

  public int getMaxConcurrentCatchUpJobs() {
    return maxConcurrentCatchUpJobs;
  }

  public void setMaxConcurrentCatchUpJobs(int maxConcurrentCatchUpJobs) {
    if (maxConcurrentCatchUpJobs <= 0) {
      throw new IllegalArgumentException("maxConcurrentCatchUpJobs must be > 0");
    }
    this.maxConcurrentCatchUpJobs = maxConcurrentCatchUpJobs;
  }

  public String getCatchUpConsumerGroupSuffix() {
    return catchUpConsumerGroupSuffix;
  }

  public void setCatchUpConsumerGroupSuffix(String catchUpConsumerGroupSuffix) {
    if (catchUpConsumerGroupSuffix == null || catchUpConsumerGroupSuffix.isEmpty()) {
      throw new IllegalArgumentException("catchUpConsumerGroupSuffix must not be empty");
    }
    this.catchUpConsumerGroupSuffix = catchUpConsumerGroupSuffix;
  }
}
