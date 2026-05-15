package com.uber.data.kafka.datatransfer.controller.rebalancer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskStatus;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;
import java.util.HashMap;
import java.util.Map;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LagMitigationRebalancerTest {

  private static final long LAG_THRESHOLD = 100_000L;
  private static final String CONSUMER_GROUP = "test-group";
  private static final String TOPIC = "test-topic";
  private static final int PARTITION = 0;
  private static final long JOB_ID = 1L;
  private static final long WORKER_ID = 42L;

  private LagMitigationConfiguration config;
  private LagDetector lagDetector;
  private Rebalancer delegate;
  private LeaderSelector leaderSelector;
  private Scope scope;
  private Counter counter;
  private LagMitigationRebalancer rebalancer;

  // ── Helpers ───────────────────────────────────────────────────────────────

  private RebalancingJobGroup buildGroup(long jobId, long commitOffset, long brokerEndOffset) {
    KafkaConsumerTask consumerTask =
        KafkaConsumerTask.newBuilder()
            .setTopic(TOPIC)
            .setPartition(PARTITION)
            .setConsumerGroup(CONSUMER_GROUP)
            .build();
    Job job = Job.newBuilder().setJobId(jobId).setKafkaConsumerTask(consumerTask).build();
    StoredJob storedJob = StoredJob.newBuilder().setJob(job).build();
    StoredJobGroup storedJobGroup =
        StoredJobGroup.newBuilder()
            .setJobGroup(
                JobGroup.newBuilder()
                    .setJobGroupId(CONSUMER_GROUP)
                    .setKafkaConsumerTaskGroup(
                        KafkaConsumerTaskGroup.newBuilder()
                            .setConsumerGroup(CONSUMER_GROUP)
                            .build())
                    .build())
            .setState(JobState.JOB_STATE_RUNNING)
            .addJobs(storedJob)
            .build();

    KafkaConsumerTaskStatus taskStatus =
        KafkaConsumerTaskStatus.newBuilder()
            .setCommitOffset(commitOffset)
            .setBrokerEndOffset(brokerEndOffset)
            .build();
    StoredJobStatus storedJobStatus =
        StoredJobStatus.newBuilder()
            .setJobStatus(JobStatus.newBuilder().setKafkaConsumerTaskStatus(taskStatus).build())
            .build();

    return RebalancingJobGroup.of(
        Versioned.from(storedJobGroup, 1), ImmutableMap.of(jobId, storedJobStatus));
  }

  private Map<Long, StoredWorker> buildWorkers() {
    StoredWorker worker =
        StoredWorker.newBuilder().setNode(Node.newBuilder().setId(WORKER_ID).build()).build();
    return ImmutableMap.of(WORKER_ID, worker);
  }

  // ── Setup ─────────────────────────────────────────────────────────────────

  @BeforeEach
  public void setup() {
    config = new LagMitigationConfiguration();
    config.setLagOffsetThreshold(LAG_THRESHOLD);
    config.setEnabled(true);

    lagDetector = new LagDetector(config);
    delegate = mock(Rebalancer.class);
    leaderSelector = mock(LeaderSelector.class);
    scope = mock(Scope.class);
    counter = mock(Counter.class);

    when(leaderSelector.isLeader()).thenReturn(true);
    when(scope.counter(org.mockito.ArgumentMatchers.anyString())).thenReturn(counter);

    rebalancer =
        new LagMitigationRebalancer(delegate, lagDetector, config, leaderSelector, scope);
  }

  // ── Observer wiring in postProcess ───────────────────────────────────────

  @Test
  public void testPostProcess_laggingJob_primesThroughObserverAndDetects() {
    // lag = 200_000 > threshold → catch-up should be created
    Map<String, RebalancingJobGroup> jobGroups = new HashMap<>();
    jobGroups.put(CONSUMER_GROUP, buildGroup(JOB_ID, 100, 200_100));
    Map<Long, StoredWorker> workers = buildWorkers();

    rebalancer.postProcess(jobGroups, workers);

    // A catch-up group must have been injected into jobGroups
    Assertions.assertEquals(2, jobGroups.size(), "catch-up group must be added to jobGroups");
    Assertions.assertEquals(1, rebalancer.getActiveCatchUpGroupIds().size());
  }

  @Test
  public void testPostProcess_nonLaggingJob_noCatchUpCreated() {
    // lag = 100 < threshold
    Map<String, RebalancingJobGroup> jobGroups = new HashMap<>();
    jobGroups.put(CONSUMER_GROUP, buildGroup(JOB_ID, 100, 200));
    Map<Long, StoredWorker> workers = buildWorkers();

    rebalancer.postProcess(jobGroups, workers);

    Assertions.assertEquals(1, jobGroups.size(), "no catch-up group should be added");
    Assertions.assertTrue(rebalancer.getActiveCatchUpGroupIds().isEmpty());
  }

  @Test
  public void testPostProcess_disabled_skipsDetectionAndMitigation() {
    config.setEnabled(false);

    Map<String, RebalancingJobGroup> jobGroups = new HashMap<>();
    jobGroups.put(CONSUMER_GROUP, buildGroup(JOB_ID, 100, 200_100));

    rebalancer.postProcess(jobGroups, buildWorkers());

    Assertions.assertEquals(1, jobGroups.size());
    Assertions.assertTrue(rebalancer.getActiveCatchUpGroupIds().isEmpty());
  }

  @Test
  public void testPostProcess_notLeader_skipsDetectionAndMitigation() {
    when(leaderSelector.isLeader()).thenReturn(false);

    Map<String, RebalancingJobGroup> jobGroups = new HashMap<>();
    jobGroups.put(CONSUMER_GROUP, buildGroup(JOB_ID, 100, 200_100));

    rebalancer.postProcess(jobGroups, buildWorkers());

    Assertions.assertEquals(1, jobGroups.size());
    Assertions.assertTrue(rebalancer.getActiveCatchUpGroupIds().isEmpty());
  }

  // ── resetIndex between cycles ─────────────────────────────────────────────

  @Test
  public void testPostProcess_secondCycleWithRecoveredJob_noCatchUpCreated() {
    // Cycle 1: job is lagging → catch-up created
    Map<String, RebalancingJobGroup> cycle1 = new HashMap<>();
    cycle1.put(CONSUMER_GROUP, buildGroup(JOB_ID, 100, 200_100));
    rebalancer.postProcess(cycle1, buildWorkers());
    Assertions.assertEquals(1, rebalancer.getActiveCatchUpGroupIds().size());

    // Cycle 2: fresh job groups, job has recovered (no lag).
    // The catch-up group from cycle 1 is absent here, so cleanUpCompletedCatchUpGroups removes it.
    // resetIndex() ensures the stale lagging state from cycle 1 is not carried forward,
    // so detect() must return empty and no new catch-up group is spawned.
    Map<String, RebalancingJobGroup> cycle2 = new HashMap<>();
    cycle2.put(CONSUMER_GROUP, buildGroup(JOB_ID, 100, 200)); // recovered
    rebalancer.postProcess(cycle2, buildWorkers());

    // activeCatchUpGroupIds is 0: cycle 1's group was cleaned up (absent from cycle 2 map)
    // and no new catch-up was created because lag is gone.
    Assertions.assertEquals(
        0,
        rebalancer.getActiveCatchUpGroupIds().size(),
        "resetIndex must prevent stale lag from cycle 1 triggering a second catch-up");
    // The cycle 2 map must still have only the one original group — no catch-up injected
    Assertions.assertEquals(1, cycle2.size());
  }

  @Test
  public void testPostProcess_catchUpGroupIdIsDeterministic() {
    // The catch-up group ID must be derived from (consumerGroup, topic, partition) so that
    // repeated postProcess calls recognise "already active" and do not spawn duplicates.
    Map<String, RebalancingJobGroup> jobGroups1 = new HashMap<>();
    jobGroups1.put(CONSUMER_GROUP, buildGroup(JOB_ID, 100, 200_100));
    rebalancer.postProcess(jobGroups1, buildWorkers());
    String catchUpId1 = rebalancer.getActiveCatchUpGroupIds().iterator().next();

    // Same topology in next cycle — catch-up is already active, must not duplicate.
    Map<String, RebalancingJobGroup> jobGroups2 = new HashMap<>();
    RebalancingJobGroup freshGroup = buildGroup(JOB_ID, 100, 200_100);
    jobGroups2.put(CONSUMER_GROUP, freshGroup);
    // Re-add the catch-up group so cleanUp sees it and it counts toward the active set
    jobGroups2.put(catchUpId1, jobGroups1.get(catchUpId1));
    rebalancer.postProcess(jobGroups2, buildWorkers());

    Assertions.assertEquals(
        1,
        rebalancer.getActiveCatchUpGroupIds().size(),
        "Duplicate catch-up must not be created for an already-active partition");
  }

  // ── maxConcurrentCatchUpJobs ──────────────────────────────────────────────

  @Test
  public void testPostProcess_respectsMaxConcurrentCatchUpJobsLimit() {
    config.setMaxConcurrentCatchUpJobs(1);

    // Two lagging jobs in separate groups
    Map<String, RebalancingJobGroup> jobGroups = new HashMap<>();
    jobGroups.put("group-a", buildGroup(1L, 100, 200_100));
    jobGroups.put("group-b", buildGroup(2L, 100, 200_100));

    rebalancer.postProcess(jobGroups, buildWorkers());

    // Only one catch-up should have been created because the limit is 1
    Assertions.assertEquals(1, rebalancer.getActiveCatchUpGroupIds().size());
  }

  // ── delegate delegation ───────────────────────────────────────────────────

  @Test
  public void testPostProcess_alwaysCallsDelegateFirst() throws Exception {
    Map<String, RebalancingJobGroup> jobGroups = new HashMap<>();
    jobGroups.put(CONSUMER_GROUP, buildGroup(JOB_ID, 100, 200));
    Map<Long, StoredWorker> workers = buildWorkers();

    rebalancer.postProcess(jobGroups, workers);

    org.mockito.Mockito.verify(delegate).postProcess(jobGroups, workers);
  }

  @Test
  public void testPostProcess_delegateCalledEvenWhenDisabled() throws Exception {
    config.setEnabled(false);
    Map<String, RebalancingJobGroup> jobGroups = new HashMap<>();
    jobGroups.put(CONSUMER_GROUP, buildGroup(JOB_ID, 100, 200));
    Map<Long, StoredWorker> workers = buildWorkers();

    rebalancer.postProcess(jobGroups, workers);

    org.mockito.Mockito.verify(delegate).postProcess(jobGroups, workers);
  }

  // ── catchUpGroupId helper ─────────────────────────────────────────────────

  @Test
  public void testCatchUpGroupId_includesConsumerGroupTopicAndPartition() {
    RebalancingJobGroup group = buildGroup(JOB_ID, 100, 200_100);
    Map<String, RebalancingJobGroup> jobGroups = new HashMap<>();
    jobGroups.put(CONSUMER_GROUP, group);

    rebalancer.postProcess(jobGroups, buildWorkers());

    String catchUpId = rebalancer.getActiveCatchUpGroupIds().iterator().next();
    Assertions.assertTrue(catchUpId.contains(CONSUMER_GROUP), "catch-up ID must contain consumer group");
    Assertions.assertTrue(catchUpId.contains(TOPIC), "catch-up ID must contain topic");
    Assertions.assertTrue(catchUpId.contains(String.valueOf(PARTITION)), "catch-up ID must contain partition");
  }
}