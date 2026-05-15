package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.m3.tally.Scope;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LagMitigationRebalancer is a {@link Rebalancer} decorator that transparently adds consumer-lag
 * mitigation on top of any existing rebalancer.
 *
 * <h2>Strategy</h2>
 *
 * When a partition's consumer lag exceeds the configured threshold the rebalancer:
 *
 * <ol>
 *   <li><b>Creates a bounded catch-up job</b> in a new job group that uses a separate consumer
 *       group (e.g. {@code mygroup-catchup}). The job consumes the lagging range
 *       {@code [committedOffset, brokerEndOffset)} and then stops automatically because
 *       {@code end_offset} is set.
 *   <li><b>Replaces the main job</b> with an updated version that has {@code start_offset =
 *       brokerEndOffset} and {@link AutoOffsetResetPolicy#AUTO_OFFSET_RESET_POLICY_EARLIEST}. The
 *       worker detects the new job ID as a fresh partition assignment and seeks directly to
 *       {@code brokerEndOffset}, skipping the lagged range entirely.
 * </ol>
 *
 * <h2>Lifecycle</h2>
 *
 * Catch-up job groups are tracked in {@link #activeCatchUpGroupIds}. During each {@link
 * #postProcess} call the rebalancer removes catch-up groups whose jobs are all reported as {@link
 * JobState#JOB_STATE_CANCELED} (i.e. bounded consumption completed).
 *
 * <h2>Integration note</h2>
 *
 * New catch-up job groups added to the {@code jobGroups} map during {@code postProcess} must be
 * persisted by the controller's job-group store. The controller should treat any group present in
 * the map at the end of the rebalancing cycle as authoritative.
 */
public class LagMitigationRebalancer implements Rebalancer {

  private static final Logger logger = LoggerFactory.getLogger(LagMitigationRebalancer.class);

  // Job ID namespace offset for catch-up jobs to avoid collision with regular job IDs.
  // Catch-up job IDs are generated as: Long.MAX_VALUE / 2 + ThreadLocalRandom value.
  private static final long CATCH_UP_JOB_ID_OFFSET = Long.MAX_VALUE / 2;

  private final Rebalancer delegate;
  private final LagDetector lagDetector;
  private final LagMitigationConfiguration config;
  private final LeaderSelector leaderSelector;
  private final Scope scope;

  // Tracks job group IDs of active catch-up job groups so we can clean them up when complete.
  // Using a ConcurrentHashMap as a set for thread safety.
  private final Set<String> activeCatchUpGroupIds = ConcurrentHashMap.newKeySet();

  public LagMitigationRebalancer(
      Rebalancer delegate,
      LagDetector lagDetector,
      LagMitigationConfiguration config,
      LeaderSelector leaderSelector,
      Scope scope) {
    this.delegate = delegate;
    this.lagDetector = lagDetector;
    this.config = config;
    this.leaderSelector = leaderSelector;
    this.scope = scope;
  }

  // ─── Delegate all standard Rebalancer methods ────────────────────────────

  @Override
  public void computeWorkerId(
      Map<String, RebalancingJobGroup> jobGroups, Map<Long, StoredWorker> workers)
      throws Exception {
    delegate.computeWorkerId(jobGroups, workers);
  }

  @Override
  public void computeJobConfiguration(
      Map<String, RebalancingJobGroup> jobGroups, Map<Long, StoredWorker> workers)
      throws Exception {
    delegate.computeJobConfiguration(jobGroups, workers);
  }

  @Override
  public void computeJobState(
      Map<String, RebalancingJobGroup> jobGroups, Map<Long, StoredWorker> workers)
      throws Exception {
    delegate.computeJobState(jobGroups, workers);
  }

  @Override
  public void computeLoad(Map<String, RebalancingJobGroup> jobGroups) {
    delegate.computeLoad(jobGroups);
  }

  // ─── Lag mitigation logic ────────────────────────────────────────────────

  @Override
  public void postProcess(
      Map<String, RebalancingJobGroup> jobGroups, Map<Long, StoredWorker> workers) {
    // Always run the delegate's post-processing first.
    delegate.postProcess(jobGroups, workers);

    if (!config.isEnabled()) {
      return;
    }
    if (!leaderSelector.isLeader()) {
      logger.debug("skipped lag mitigation postProcess: not leader");
      return;
    }

    // Step 1: Clean up completed catch-up job groups before detecting new lag so that their
    // slots count toward maxConcurrentCatchUpJobs correctly.
    cleanUpCompletedCatchUpGroups(jobGroups);

    // Step 2: Prime the LagDetector's index for this cycle via the Observer pattern.
    // RebalancingJobGroup instances are ephemeral (recreated each cycle), so we register
    // the detector, replay all current statuses, then immediately unregister to keep the
    // observer list clean. onStatusUpdate() builds the laggingIndex in O(1) per job.
    lagDetector.resetIndex();
    for (RebalancingJobGroup group : jobGroups.values()) {
      group.addObserver(lagDetector);
      for (Map.Entry<Long, StoredJobStatus> entry : group.getJobStatusMap().entrySet()) {
        group.updateJobStatus(
            entry.getKey(),
            entry.getValue().getJobStatus().getKafkaConsumerTaskStatus());
      }
      group.removeObserver(lagDetector);
    }

    // Step 3: Detect lagging partitions — reads laggingIndex, O(L).
    List<LagMitigationEvent> lagEvents = lagDetector.detect(jobGroups);
    if (lagEvents.isEmpty()) {
      return;
    }

    // Step 4: Apply lag mitigation up to the configured concurrency limit.
    int activeCatchUpCount = activeCatchUpGroupIds.size();
    for (LagMitigationEvent event : lagEvents) {
      if (activeCatchUpCount >= config.getMaxConcurrentCatchUpJobs()) {
        logger.warn(
            "lag mitigation skipped: reached maxConcurrentCatchUpJobs limit",
            StructuredLogging.count(config.getMaxConcurrentCatchUpJobs()));
        break;
      }
      String catchUpGroupId = catchUpGroupId(event);
      if (activeCatchUpGroupIds.contains(catchUpGroupId)) {
        // Catch-up already in progress for this partition — skip.
        continue;
      }

      try {
        applyMitigation(event, jobGroups, workers, catchUpGroupId);
        activeCatchUpGroupIds.add(catchUpGroupId);
        activeCatchUpCount++;
        logger.info(
            "lag mitigation activated",
            StructuredLogging.kafkaGroup(
                event.getMainJob().getKafkaConsumerTask().getConsumerGroup()),
            StructuredLogging.kafkaTopic(event.getMainJob().getKafkaConsumerTask().getTopic()),
            StructuredLogging.kafkaPartition(
                event.getMainJob().getKafkaConsumerTask().getPartition()),
            StructuredLogging.kafkaOffset(event.getCommittedOffset()),
            StructuredLogging.reason(
                "brokerEndOffset=" + event.getBrokerEndOffset()
                    + " lag=" + event.getLagMessages()));
        scope.counter(MetricNames.CATCH_UP_ACTIVATED).inc(1);
      } catch (Exception e) {
        logger.error(
            "failed to apply lag mitigation",
            StructuredLogging.kafkaTopic(event.getMainJob().getKafkaConsumerTask().getTopic()),
            StructuredLogging.kafkaPartition(
                event.getMainJob().getKafkaConsumerTask().getPartition()),
            e);
      }
    }
  }

  // ─── Private helpers ─────────────────────────────────────────────────────

  /**
   * Core mitigation logic for a single lagging partition:
   *
   * <ol>
   *   <li>Replace the main job with a new job that seeks to {@code brokerEndOffset}.
   *   <li>Inject a bounded catch-up job group that replays {@code [committedOffset,
   *       brokerEndOffset)}.
   * </ol>
   */
  private void applyMitigation(
      LagMitigationEvent event,
      Map<String, RebalancingJobGroup> jobGroups,
      Map<Long, StoredWorker> workers,
      String catchUpGroupId) {

    Job mainJob = event.getMainJob();
    long committedOffset = event.getCommittedOffset();
    long brokerEndOffset = event.getBrokerEndOffset();

    // ── 1. Locate the job group that owns the lagging main job ──────────────
    Optional<RebalancingJobGroup> ownerGroupOpt =
        jobGroups.values().stream()
            .filter(jg -> jg.getJobs().containsKey(mainJob.getJobId()))
            .findFirst();
    if (!ownerGroupOpt.isPresent()) {
      logger.warn(
          "lag mitigation: could not find owner job group for job",
          StructuredLogging.jobId(mainJob.getJobId()));
      return;
    }
    RebalancingJobGroup ownerGroup = ownerGroupOpt.get();
    StoredJob oldStoredJob = ownerGroup.getJobs().get(mainJob.getJobId());

    // ── 2. Replace main job with a version that seeks to latest offset ───────
    long newMainJobId = generateJobId();
    Job updatedMainJob =
        mainJob.toBuilder()
            .setJobId(newMainJobId)
            .setKafkaConsumerTask(
                mainJob.getKafkaConsumerTask().toBuilder()
                    .setStartOffset(brokerEndOffset)
                    // EARLIEST: honor the explicit start_offset (= brokerEndOffset) set above.
                    .setAutoOffsetResetPolicy(
                        AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST)
                    .build())
            .build();
    StoredJob newMainStoredJob = oldStoredJob.toBuilder().setJob(updatedMainJob).build();

    ownerGroup.removeJob(mainJob.getJobId());
    ownerGroup.addJob(newMainStoredJob);

    // ── 3. Build the catch-up job ─────────────────────────────────────────────
    String catchUpConsumerGroup =
        mainJob.getKafkaConsumerTask().getConsumerGroup()
            + config.getCatchUpConsumerGroupSuffix();
    long catchUpJobId = generateJobId();
    Job catchUpJob =
        mainJob.toBuilder()
            .setJobId(catchUpJobId)
            .setKafkaConsumerTask(
                mainJob.getKafkaConsumerTask().toBuilder()
                    .setConsumerGroup(catchUpConsumerGroup)
                    .setStartOffset(committedOffset)
                    // end_offset is exclusive per Kafka semantics — stop just before brokerEndOffset
                    // so we don't overlap with the main consumer.
                    .setEndOffset(brokerEndOffset)
                    .setAutoOffsetResetPolicy(
                        AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST)
                    .build())
            .build();
    StoredJob catchUpStoredJob =
        oldStoredJob.toBuilder()
            .setJob(catchUpJob)
            // Assign to a random live worker; the rebalancer will redistribute if needed.
            .setWorkerId(pickWorkerId(workers))
            .build();

    // ── 4. Build a catch-up job group and inject it into the jobGroups map ───
    RebalancingJobGroup catchUpGroup = buildCatchUpJobGroup(ownerGroup, catchUpGroupId,
        catchUpConsumerGroup, catchUpStoredJob);
    jobGroups.put(catchUpGroupId, catchUpGroup);
  }

  /**
   * Removes catch-up job groups from the map (and from {@link #activeCatchUpGroupIds}) once all
   * their jobs are reported as {@link JobState#JOB_STATE_CANCELED} (bounded consumption done).
   */
  private void cleanUpCompletedCatchUpGroups(Map<String, RebalancingJobGroup> jobGroups) {
    List<String> completed = new ArrayList<>();
    for (String catchUpGroupId : activeCatchUpGroupIds) {
      RebalancingJobGroup group = jobGroups.get(catchUpGroupId);
      if (group == null) {
        // Group was already removed externally.
        completed.add(catchUpGroupId);
        continue;
      }
      boolean allDone =
          group.getJobStatusMap().values().stream()
              .allMatch(
                  status ->
                      status.getJobStatus().getState() == JobState.JOB_STATE_CANCELED
                          || status.getJobStatus().getState() == JobState.JOB_STATE_FAILED);
      if (allDone && !group.getJobStatusMap().isEmpty()) {
        jobGroups.remove(catchUpGroupId);
        completed.add(catchUpGroupId);
        logger.info("lag mitigation catch-up completed, removing group", StructuredLogging.kafkaGroup(catchUpGroupId));
        scope.counter(MetricNames.CATCH_UP_COMPLETED).inc(1);
      }
    }
    activeCatchUpGroupIds.removeAll(completed);
  }

  /**
   * Constructs a new {@link RebalancingJobGroup} for the catch-up consumer. The group mirrors the
   * main group's {@link JobGroup} configuration (RPC target, retry config, etc.) but uses the
   * catch-up consumer group name.
   */
  private RebalancingJobGroup buildCatchUpJobGroup(
      RebalancingJobGroup mainGroup,
      String catchUpGroupId,
      String catchUpConsumerGroup,
      StoredJob catchUpStoredJob) {

    JobGroup mainJobGroup = mainGroup.getJobGroup();
    // Build the catch-up JobGroup: same dispatch config, but different consumer group.
    KafkaConsumerTaskGroup catchUpTaskGroup =
        mainJobGroup.getKafkaConsumerTaskGroup().toBuilder()
            .setConsumerGroup(catchUpConsumerGroup)
            .build();
    JobGroup catchUpJobGroup =
        mainJobGroup.toBuilder()
            .setJobGroupId(catchUpGroupId)
            .setKafkaConsumerTaskGroup(catchUpTaskGroup)
            .build();
    StoredJobGroup catchUpStoredJobGroup =
        StoredJobGroup.newBuilder()
            .setJobGroup(catchUpJobGroup)
            .setState(JobState.JOB_STATE_RUNNING)
            .addJobs(catchUpStoredJob)
            .build();
    // version=0 signals a newly-created group; the controller's store should assign a real version.
    return RebalancingJobGroup.of(
        Versioned.from(catchUpStoredJobGroup, 0), Collections.emptyMap());
  }

  /** Deterministic catch-up group ID derived from the main job's group and partition. */
  @VisibleForTesting
  String catchUpGroupId(LagMitigationEvent event) {
    Job job = event.getMainJob();
    // Build a stable ID scoped to the consumer group + topic + partition triple so that repeated
    // rebalancing cycles detect "already active" correctly without persisted state.
    return job.getKafkaConsumerTask().getConsumerGroup()
        + config.getCatchUpConsumerGroupSuffix()
        + "-"
        + job.getKafkaConsumerTask().getTopic()
        + "-"
        + job.getKafkaConsumerTask().getPartition();
  }

  /** Picks a random live worker ID for the catch-up job's initial placement. */
  private long pickWorkerId(Map<Long, StoredWorker> workers) {
    if (workers.isEmpty()) {
      return 0L;
    }
    List<Long> ids = new ArrayList<>(workers.keySet());
    return ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
  }

  /**
   * Generates a unique job ID in the catch-up namespace ({@code Long.MAX_VALUE/2 .. Long.MAX_VALUE}
   * ) to avoid collisions with the controller's regular sequential IDs.
   */
  private long generateJobId() {
    return CATCH_UP_JOB_ID_OFFSET
        + ThreadLocalRandom.current().nextLong(CATCH_UP_JOB_ID_OFFSET);
  }

  @VisibleForTesting
  Set<String> getActiveCatchUpGroupIds() {
    return Collections.unmodifiableSet(activeCatchUpGroupIds);
  }

  private static class MetricNames {
    static final String CATCH_UP_COMPLETED = "lag.mitigation.catchup.completed";
    static final String CATCH_UP_ACTIVATED = "lag.mitigation.catchup.activated";
  }
}
