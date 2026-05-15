package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * LagDetector scans all job groups to find partitions whose consumer lag exceeds the configured
 * threshold.
 *
 * <p>It is intentionally stateless: every call to {@link #detect} produces a fresh snapshot of
 * lagging partitions based on the current job status reported by workers. This makes it safe to
 * call from any scheduling context without concurrency concerns.
 *
 * <p>Lag is computed as:
 *
 * <pre>
 *   lag = broker_end_offset - commit_offset
 * </pre>
 *
 * where {@code broker_end_offset} is populated by the worker in {@link KafkaConsumerTaskStatus}
 * (see AbstractKafkaFetcherThread) and {@code commit_offset} is the last offset the worker
 * successfully committed to Kafka.
 *
 * <p>Partitions are skipped when:
 *
 * <ul>
 *   <li>{@code broker_end_offset} was never reported (value = -1 or 0).
 *   <li>{@code commit_offset} is not yet valid (value <= 0).
 *   <li>Computed lag does not exceed {@link LagMitigationConfiguration#getLagOffsetThreshold()}.
 * </ul>
 */
public class LagDetector {

  private final LagMitigationConfiguration config;

  public LagDetector(LagMitigationConfiguration config) {
    this.config = config;
  }

  /**
   * Scans every job in every job group and returns one {@link LagMitigationEvent} per partition
   * whose lag exceeds the configured threshold.
   *
   * @param jobGroups snapshot of all job groups from the current rebalancing cycle
   * @return list of lag events; empty when no partition is lagging beyond the threshold
   */
  public List<LagMitigationEvent> detect(Map<String, RebalancingJobGroup> jobGroups) {
    List<LagMitigationEvent> events = new ArrayList<>();
    for (RebalancingJobGroup jobGroup : jobGroups.values()) {
      Map<Long, StoredJobStatus> statusMap = jobGroup.getJobStatusMap();
      for (Map.Entry<Long, StoredJob> jobEntry : jobGroup.getJobs().entrySet()) {
        StoredJob storedJob = jobEntry.getValue();
        Job job = storedJob.getJob();
        StoredJobStatus storedJobStatus = statusMap.get(jobEntry.getKey());
        if (storedJobStatus == null) {
          continue;
        }
        KafkaConsumerTaskStatus taskStatus =
            storedJobStatus.getJobStatus().getKafkaConsumerTaskStatus();
        long brokerEndOffset = taskStatus.getBrokerEndOffset();
        long committedOffset = taskStatus.getCommitOffset();

        // Skip partitions where the broker end offset has not been reported yet.
        if (brokerEndOffset <= 0) {
          continue;
        }
        // Skip partitions where the commit offset is not yet valid.
        if (committedOffset <= 0) {
          continue;
        }

        long lag = brokerEndOffset - committedOffset;
        if (lag > config.getLagOffsetThreshold()) {
          events.add(LagMitigationEvent.of(job, committedOffset, brokerEndOffset));
        }
      }
    }
    return events;
  }
}