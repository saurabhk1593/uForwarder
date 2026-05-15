package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.uber.data.kafka.datatransfer.Job;

/**
 * Represents a detected consumer-lag situation on a single Kafka partition.
 *
 * <p>Created by {@link LagDetector} when a partition's lag exceeds the configured threshold. The
 * event carries the offsets needed to split processing into two ranges:
 *
 * <ul>
 *   <li><b>catch-up range</b>: {@code [committedOffset, brokerEndOffset)} — processed by the
 *       bounded catch-up consumer to achieve data completeness.
 *   <li><b>fresh range</b>: {@code [brokerEndOffset, ∞)} — processed by the main consumer (after
 *       rewinding to the latest) to achieve data freshness.
 * </ul>
 */
public final class LagMitigationEvent {

  /** The main job that is lagging. */
  private final Job mainJob;

  /**
   * The committed offset of the main consumer at detection time. This is where the catch-up
   * consumer will start reading.
   */
  private final long committedOffset;

  /**
   * The broker log-end offset at detection time. The catch-up consumer will stop here, and the
   * main consumer will seek to this offset to process fresh messages.
   */
  private final long brokerEndOffset;

  private LagMitigationEvent(Job mainJob, long committedOffset, long brokerEndOffset) {
    this.mainJob = mainJob;
    this.committedOffset = committedOffset;
    this.brokerEndOffset = brokerEndOffset;
  }

  /**
   * Creates a new LagMitigationEvent.
   *
   * @param mainJob the lagging job
   * @param committedOffset committed offset at detection time (catch-up start)
   * @param brokerEndOffset broker end offset at detection time (catch-up end / main-consumer seek
   *     target)
   */
  public static LagMitigationEvent of(Job mainJob, long committedOffset, long brokerEndOffset) {
    return new LagMitigationEvent(mainJob, committedOffset, brokerEndOffset);
  }

  public Job getMainJob() {
    return mainJob;
  }

  /** Offset at which the catch-up consumer should start (inclusive). */
  public long getCommittedOffset() {
    return committedOffset;
  }

  /**
   * Offset at which the catch-up consumer should stop (exclusive per Kafka semantics). This is
   * also the offset the main consumer should seek to in order to skip the lagging range.
   */
  public long getBrokerEndOffset() {
    return brokerEndOffset;
  }

  /** Lag size in messages at detection time. */
  public long getLagMessages() {
    return brokerEndOffset - committedOffset;
  }

  @Override
  public String toString() {
    return "LagMitigationEvent{"
        + "jobId="
        + mainJob.getJobId()
        + ", topic="
        + mainJob.getKafkaConsumerTask().getTopic()
        + ", partition="
        + mainJob.getKafkaConsumerTask().getPartition()
        + ", committedOffset="
        + committedOffset
        + ", brokerEndOffset="
        + brokerEndOffset
        + ", lagMessages="
        + getLagMessages()
        + '}';
  }
}