package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LagDetector maintains an incremental index of lagging partitions, updated via the {@link
 * JobStatusObserver} interface instead of performing a full scan on every rebalancing cycle.
 *
 * <h2>How it works</h2>
 *
 * <ol>
 *   <li>At the start of each rebalancing cycle {@link LagMitigationRebalancer#postProcess} calls
 *       {@link #resetIndex()} to discard stale data from the previous cycle.
 *   <li>It then registers this detector as an observer on each {@link RebalancingJobGroup} and
 *       calls {@link RebalancingJobGroup#updateJobStatus} for every job status in the group. Each
 *       call dispatches to {@link #onStatusUpdate}, which classifies the partition and updates
 *       {@link #laggingIndex} in O(1).
 *   <li>Finally {@link #detect} reads {@link #laggingIndex} — one lookup per group, O(L) where L
 *       is the number of lagging partitions — and constructs {@link LagMitigationEvent}s.
 * </ol>
 *
 * <h2>Lag formula</h2>
 *
 * <pre>
 *   lag = broker_end_offset - commit_offset
 * </pre>
 *
 * where {@code broker_end_offset} is populated by the worker in {@link KafkaConsumerTaskStatus}
 * (see AbstractKafkaFetcherThread) and {@code commit_offset} is the last offset successfully
 * committed to Kafka.
 *
 * <p>Partitions are skipped (not added to the index) when:
 *
 * <ul>
 *   <li>{@code broker_end_offset} was never reported (value &le; 0).
 *   <li>{@code commit_offset} is not yet valid (value &le; 0).
 *   <li>Computed lag does not exceed {@link LagMitigationConfiguration#getLagOffsetThreshold()}.
 * </ul>
 */
public class LagDetector implements JobStatusObserver {

  private final LagMitigationConfiguration config;

  /**
   * Incremental index: maps each job group to the set of job IDs within it that are currently
   * classified as lagging. Rebuilt from scratch at the start of every rebalancing cycle via {@link
   * #resetIndex()} followed by a replay of all job statuses through {@link #onStatusUpdate}.
   *
   * <p>Keys are {@link RebalancingJobGroup} instances from the current cycle's {@code jobGroups}
   * map; values are concurrent sets of lagging job IDs. Because {@link #resetIndex()} clears the
   * map at the start of every cycle, stale references from previous cycles never accumulate.
   */
  final Map<RebalancingJobGroup, Set<Long>> laggingIndex = new ConcurrentHashMap<>();

  public LagDetector(LagMitigationConfiguration config) {
    this.config = config;
  }

  /**
   * Clears the lagging index. Must be called once at the start of each rebalancing cycle, before
   * any {@link RebalancingJobGroup#updateJobStatus} calls are made, to ensure stale entries from
   * the previous cycle do not bleed into the new one.
   */
  public void resetIndex() {
    laggingIndex.clear();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Classifies {@code jobId} within {@code group} as lagging or non-lagging based on the
   * current {@link KafkaConsumerTaskStatus} and updates {@link #laggingIndex} accordingly. This
   * runs in O(1) per call.
   */
  @Override
  public void onStatusUpdate(
      RebalancingJobGroup group, long jobId, KafkaConsumerTaskStatus taskStatus) {
    long brokerEndOffset = taskStatus.getBrokerEndOffset();
    long committedOffset = taskStatus.getCommitOffset();

    // Skip partitions where offsets are not yet valid.
    if (brokerEndOffset <= 0 || committedOffset <= 0) {
      return;
    }

    boolean isLagging = (brokerEndOffset - committedOffset) > config.getLagOffsetThreshold();

    if (isLagging) {
      laggingIndex.computeIfAbsent(group, g -> ConcurrentHashMap.newKeySet()).add(jobId);
    } else {
      Set<Long> lagging = laggingIndex.get(group);
      if (lagging != null) {
        lagging.remove(jobId);
      }
    }
  }

  /**
   * Reads {@link #laggingIndex} and returns one {@link LagMitigationEvent} per partition currently
   * classified as lagging. This is an O(L) operation where L is the number of lagging partitions —
   * no per-job status scanning is performed here.
   *
   * <p>Must be called after {@link #resetIndex()} and a full replay of job statuses via {@link
   * RebalancingJobGroup#updateJobStatus} for the current rebalancing cycle.
   *
   * @param jobGroups snapshot of all job groups from the current rebalancing cycle
   * @return list of lag events; empty when no partition is lagging beyond the threshold
   */
  public List<LagMitigationEvent> detect(Map<String, RebalancingJobGroup> jobGroups) {
    List<LagMitigationEvent> events = new ArrayList<>();

    for (RebalancingJobGroup group : jobGroups.values()) {
      Set<Long> laggingIds = laggingIndex.getOrDefault(group, Collections.emptySet());

      for (long jobId : laggingIds) {
        StoredJob storedJob = group.getJobs().get(jobId);
        StoredJobStatus storedJobStatus = group.getJobStatusMap().get(jobId);
        if (storedJob == null || storedJobStatus == null) {
          continue;
        }

        Job job = storedJob.getJob();
        KafkaConsumerTaskStatus taskStatus =
            storedJobStatus.getJobStatus().getKafkaConsumerTaskStatus();

        events.add(
            LagMitigationEvent.of(job, taskStatus.getCommitOffset(), taskStatus.getBrokerEndOffset()));
      }
    }
    return events;
  }
}