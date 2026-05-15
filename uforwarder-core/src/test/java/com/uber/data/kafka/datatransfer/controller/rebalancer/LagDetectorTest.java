package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import java.util.List;
import java.util.Map;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LagDetectorTest {

  private static final long LAG_THRESHOLD = 100_000L;
  private static final long JOB_ID = 1L;
  private static final String GROUP_ID = "test-group";

  private LagMitigationConfiguration config;
  private LagDetector lagDetector;

  // ── Helpers ───────────────────────────────────────────────────────────────

  /** Builds a minimal {@link RebalancingJobGroup} with one job and the supplied task status. */
  private RebalancingJobGroup buildGroup(long jobId, KafkaConsumerTaskStatus taskStatus) {
    KafkaConsumerTask consumerTask =
        KafkaConsumerTask.newBuilder()
            .setTopic("test-topic")
            .setPartition(0)
            .setConsumerGroup("test-consumer-group")
            .build();
    Job job = Job.newBuilder().setJobId(jobId).setKafkaConsumerTask(consumerTask).build();
    StoredJob storedJob = StoredJob.newBuilder().setJob(job).build();
    StoredJobGroup storedJobGroup =
        StoredJobGroup.newBuilder()
            .setState(JobState.JOB_STATE_RUNNING)
            .addJobs(storedJob)
            .build();

    StoredJobStatus storedJobStatus =
        StoredJobStatus.newBuilder()
            .setJobStatus(JobStatus.newBuilder().setKafkaConsumerTaskStatus(taskStatus).build())
            .build();

    return RebalancingJobGroup.of(
        Versioned.from(storedJobGroup, 1), ImmutableMap.of(jobId, storedJobStatus));
  }

  /** Returns a {@link KafkaConsumerTaskStatus} with the given offsets. */
  private static KafkaConsumerTaskStatus taskStatus(long commitOffset, long brokerEndOffset) {
    return KafkaConsumerTaskStatus.newBuilder()
        .setCommitOffset(commitOffset)
        .setBrokerEndOffset(brokerEndOffset)
        .build();
  }

  // ── Setup ─────────────────────────────────────────────────────────────────

  @BeforeEach
  public void setup() {
    config = new LagMitigationConfiguration();
    config.setLagOffsetThreshold(LAG_THRESHOLD);
    lagDetector = new LagDetector(config);
  }

  // ── onStatusUpdate ────────────────────────────────────────────────────────

  @Test
  public void testOnStatusUpdate_laggingPartition_addedToIndex() {
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(100, 100_101)); // lag = 100_001 > threshold

    lagDetector.onStatusUpdate(group, JOB_ID, taskStatus(100, 100_101));

    Assertions.assertTrue(lagDetector.laggingIndex.containsKey(group));
    Assertions.assertTrue(lagDetector.laggingIndex.get(group).contains(JOB_ID));
  }

  @Test
  public void testOnStatusUpdate_lagExactlyAtThreshold_notAddedToIndex() {
    // lag = threshold (not strictly greater), should NOT trigger
    lagDetector.onStatusUpdate(
        buildGroup(JOB_ID, taskStatus(100, 100_100)), JOB_ID, taskStatus(100, 100_100));

    Assertions.assertFalse(lagDetector.laggingIndex.containsKey(buildGroup(JOB_ID, taskStatus(100, 100_100))));
  }

  @Test
  public void testOnStatusUpdate_lagBelowThreshold_notAddedToIndex() {
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(100, 200)); // lag = 100 < threshold

    lagDetector.onStatusUpdate(group, JOB_ID, taskStatus(100, 200));

    Assertions.assertFalse(lagDetector.laggingIndex.containsKey(group));
  }

  @Test
  public void testOnStatusUpdate_partitionRecovery_removedFromIndex() {
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(100, 100_101));

    // First: mark as lagging
    lagDetector.onStatusUpdate(group, JOB_ID, taskStatus(100, 100_101));
    Assertions.assertTrue(lagDetector.laggingIndex.get(group).contains(JOB_ID));

    // Then: status recovers below threshold
    lagDetector.onStatusUpdate(group, JOB_ID, taskStatus(100, 200));
    Assertions.assertFalse(lagDetector.laggingIndex.get(group).contains(JOB_ID));
  }

  @Test
  public void testOnStatusUpdate_zeroBrokerEndOffset_skipped() {
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(100, 0));

    lagDetector.onStatusUpdate(group, JOB_ID, taskStatus(100, 0));

    Assertions.assertFalse(lagDetector.laggingIndex.containsKey(group));
  }

  @Test
  public void testOnStatusUpdate_negativeBrokerEndOffset_skipped() {
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(100, -1));

    lagDetector.onStatusUpdate(group, JOB_ID, taskStatus(100, -1));

    Assertions.assertFalse(lagDetector.laggingIndex.containsKey(group));
  }

  @Test
  public void testOnStatusUpdate_zeroCommitOffset_skipped() {
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(0, 100_101));

    lagDetector.onStatusUpdate(group, JOB_ID, taskStatus(0, 100_101));

    Assertions.assertFalse(lagDetector.laggingIndex.containsKey(group));
  }

  @Test
  public void testOnStatusUpdate_multipleJobsInSameGroup() {
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(100, 100_101));

    // job 1: lagging, job 2: not lagging
    lagDetector.onStatusUpdate(group, 1L, taskStatus(100, 100_101));
    lagDetector.onStatusUpdate(group, 2L, taskStatus(100, 200));

    Assertions.assertTrue(lagDetector.laggingIndex.get(group).contains(1L));
    Assertions.assertFalse(lagDetector.laggingIndex.get(group).contains(2L));
  }

  // ── resetIndex ────────────────────────────────────────────────────────────

  @Test
  public void testResetIndex_clearsAllEntries() {
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(100, 100_101));
    lagDetector.onStatusUpdate(group, JOB_ID, taskStatus(100, 100_101));
    Assertions.assertFalse(lagDetector.laggingIndex.isEmpty());

    lagDetector.resetIndex();

    Assertions.assertTrue(lagDetector.laggingIndex.isEmpty());
  }

  @Test
  public void testResetIndex_onEmptyIndex_doesNotThrow() {
    Assertions.assertDoesNotThrow(() -> lagDetector.resetIndex());
  }

  // ── detect ────────────────────────────────────────────────────────────────

  @Test
  public void testDetect_returnsEventForLaggingJob() {
    KafkaConsumerTaskStatus laggingStatus = taskStatus(100, 100_101);
    RebalancingJobGroup group = buildGroup(JOB_ID, laggingStatus);
    Map<String, RebalancingJobGroup> jobGroups = ImmutableMap.of(GROUP_ID, group);

    lagDetector.onStatusUpdate(group, JOB_ID, laggingStatus);
    List<LagMitigationEvent> events = lagDetector.detect(jobGroups);

    Assertions.assertEquals(1, events.size());
    LagMitigationEvent event = events.get(0);
    Assertions.assertEquals(JOB_ID, event.getMainJob().getJobId());
    Assertions.assertEquals(100, event.getCommittedOffset());
    Assertions.assertEquals(100_101, event.getBrokerEndOffset());
  }

  @Test
  public void testDetect_emptyWhenNoLaggingJobs() {
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(100, 200));
    Map<String, RebalancingJobGroup> jobGroups = ImmutableMap.of(GROUP_ID, group);

    // no onStatusUpdate calls → laggingIndex is empty
    List<LagMitigationEvent> events = lagDetector.detect(jobGroups);

    Assertions.assertTrue(events.isEmpty());
  }

  @Test
  public void testDetect_skipsJobsWithNoStatusEntry() {
    // job 99 is put in the lagging index but has no corresponding StoredJobStatus in the group
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(100, 100_101));

    // Prime the index for a job ID that does NOT exist in the group's status map
    lagDetector.onStatusUpdate(group, 99L, taskStatus(100, 100_101));
    List<LagMitigationEvent> events = lagDetector.detect(ImmutableMap.of(GROUP_ID, group));

    Assertions.assertTrue(events.isEmpty());
  }

  @Test
  public void testDetect_afterResetIndex_returnsEmpty() {
    KafkaConsumerTaskStatus laggingStatus = taskStatus(100, 100_101);
    RebalancingJobGroup group = buildGroup(JOB_ID, laggingStatus);

    lagDetector.onStatusUpdate(group, JOB_ID, laggingStatus);
    lagDetector.resetIndex();

    List<LagMitigationEvent> events = lagDetector.detect(ImmutableMap.of(GROUP_ID, group));
    Assertions.assertTrue(events.isEmpty());
  }

  @Test
  public void testDetect_multipleGroups_onlyLaggingReturned() {
    KafkaConsumerTaskStatus laggingStatus = taskStatus(100, 100_101);
    KafkaConsumerTaskStatus healthyStatus = taskStatus(100, 200);

    RebalancingJobGroup laggingGroup = buildGroup(1L, laggingStatus);
    RebalancingJobGroup healthyGroup = buildGroup(2L, healthyStatus);

    lagDetector.onStatusUpdate(laggingGroup, 1L, laggingStatus);
    lagDetector.onStatusUpdate(healthyGroup, 2L, healthyStatus);

    List<LagMitigationEvent> events =
        lagDetector.detect(ImmutableMap.of("group-a", laggingGroup, "group-b", healthyGroup));

    Assertions.assertEquals(1, events.size());
    Assertions.assertEquals(1L, events.get(0).getMainJob().getJobId());
  }

  @Test
  public void testDetect_lagMessageCountIsCorrect() {
    KafkaConsumerTaskStatus status = taskStatus(500, 200_500); // lag = 200_000
    RebalancingJobGroup group = buildGroup(JOB_ID, status);
    lagDetector.onStatusUpdate(group, JOB_ID, status);

    List<LagMitigationEvent> events = lagDetector.detect(ImmutableMap.of(GROUP_ID, group));

    Assertions.assertEquals(1, events.size());
    Assertions.assertEquals(200_000L, events.get(0).getLagMessages());
  }

  // ── Stale index across cycles ─────────────────────────────────────────────

  @Test
  public void testResetIndex_preventsStaleDetectionAcrossCycles() {
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(100, 100_101));

    // Cycle 1: job is lagging → lands in index
    lagDetector.onStatusUpdate(group, JOB_ID, taskStatus(100, 100_101));
    Assertions.assertEquals(1, lagDetector.detect(ImmutableMap.of(GROUP_ID, group)).size());

    // Cycle 2: reset then re-prime with recovered status → must not carry stale lag
    lagDetector.resetIndex();
    lagDetector.onStatusUpdate(group, JOB_ID, taskStatus(100, 200));

    List<LagMitigationEvent> events = lagDetector.detect(ImmutableMap.of(GROUP_ID, group));
    Assertions.assertTrue(events.isEmpty(), "Stale lag from previous cycle must not appear");
  }

  @Test
  public void testWithoutResetIndex_staleEntryPersistsAcrossCycles() {
    RebalancingJobGroup group = buildGroup(JOB_ID, taskStatus(100, 100_101));

    // Cycle 1: prime with lagging
    lagDetector.onStatusUpdate(group, JOB_ID, taskStatus(100, 100_101));

    // Cycle 2: no reset, no re-prime → stale index entry from cycle 1 remains
    List<LagMitigationEvent> events = lagDetector.detect(ImmutableMap.of(GROUP_ID, group));
    Assertions.assertEquals(
        1, events.size(), "Without resetIndex(), stale entry from cycle 1 is still visible");
  }
}