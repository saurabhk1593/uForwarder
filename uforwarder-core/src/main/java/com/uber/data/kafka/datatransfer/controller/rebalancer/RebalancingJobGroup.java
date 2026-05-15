package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskStatus;
import com.uber.data.kafka.datatransfer.ScaleStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Throughput;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RebalancingJobGroup is a mutable view of a Job Group that represents a rebalancing computation.
 */
public final class RebalancingJobGroup {
  private static final double EPSILON = 0.00001d;
  private static final Logger logger = LoggerFactory.getLogger(RebalancingJobGroup.class);
  private volatile StoredJobGroup storedJobGroup;
  private final ConcurrentMap<Long, StoredJob> jobsMap;
  private final Map<Long, StoredJobStatus> jobStatusMap;
  private final int version;
  private AtomicBoolean changed;
  private final List<JobStatusObserver> observers = new CopyOnWriteArrayList<>();

  private RebalancingJobGroup(
      StoredJobGroup storedJobGroup,
      int version,
      ConcurrentMap<Long, StoredJob> jobsMap,
      Map<Long, StoredJobStatus> jobStatusMap) {
    this.storedJobGroup = storedJobGroup;
    this.version = version;
    this.jobsMap = jobsMap;
    this.jobStatusMap = Collections.unmodifiableMap(jobStatusMap);
    this.changed = new AtomicBoolean(false);
  }

  /** Creates a new RebalancingJobGroup for a stored job group. */
  public static RebalancingJobGroup of(
      Versioned<StoredJobGroup> storedJobGroup, Map<Long, StoredJobStatus> jobStatusMap) {
    return new RebalancingJobGroup(
        storedJobGroup.model(),
        storedJobGroup.version(),
        // this map is deliberately generated to be mutable.
        storedJobGroup.model().getJobsList().stream()
            .collect(Collectors.toConcurrentMap(k -> k.getJob().getJobId(), v -> v)),
        jobStatusMap);
  }

  /** Returns true if the computation has changed from the initialized object. */
  public synchronized boolean isChanged() {
    return changed.get();
  }

  /**
   * Gets the {@link JobGroup}.
   *
   * @return an immutable JobGroup.
   */
  public synchronized JobGroup getJobGroup() {
    return storedJobGroup.getJobGroup();
  }

  /**
   * Gets the {@link JobState} for the job group.
   *
   * @return an immutable JobState.
   */
  public synchronized JobState getJobGroupState() {
    return storedJobGroup.getState();
  }

  /**
   * Gets scale of the job group
   *
   * @return the scale
   */
  public synchronized Optional<Double> getScale() {
    return storedJobGroup.hasScaleStatus()
        ? Optional.of(storedJobGroup.getScaleStatus().getScale())
        : Optional.empty();
  }

  /**
   * Gets the throughput of the job group
   *
   * @return the throughput
   */
  public synchronized Optional<Throughput> getThroughput() {
    return storedJobGroup.hasScaleStatus()
        ? Optional.of(
            new Throughput(
                storedJobGroup.getScaleStatus().getTotalMessagesPerSec(),
                storedJobGroup.getScaleStatus().getTotalBytesPerSec()))
        : Optional.empty();
  }

  /**
   * Gets the {@link StoredJob} that are currently stored in the job group.
   *
   * @return an immutable map of jobs.
   */
  public synchronized Map<Long, StoredJob> getJobs() {
    return Collections.unmodifiableMap(jobsMap);
  }

  /**
   * Gets the {@link StoredJobStatus} for each job in this {@link JobGroup}.
   *
   * @return an immutable map of job statuses.
   */
  public synchronized Map<Long, StoredJobStatus> getJobStatusMap() {
    return jobStatusMap;
  }

  /** Registers an observer to be notified on {@link #updateJobStatus} calls. */
  public void addObserver(JobStatusObserver observer) {
    observers.add(observer);
  }

  /** Removes a previously registered observer. No-op if the observer was not registered. */
  public void removeObserver(JobStatusObserver observer) {
    observers.remove(observer);
  }

  /**
   * Notifies all registered {@link JobStatusObserver}s that the given job's status has been
   * evaluated. This does not modify the stored job-status snapshot; it is purely a notification
   * hook used to drive incremental indexes such as {@link LagDetector#laggingIndex}.
   *
   * <p>Callers (e.g. {@link LagMitigationRebalancer}) invoke this once per job status at the start
   * of each rebalancing cycle to prime any registered observers before querying them.
   *
   * @param jobId the job whose status is being replayed
   * @param taskStatus the latest {@link KafkaConsumerTaskStatus} for the job
   */
  public void updateJobStatus(long jobId, KafkaConsumerTaskStatus taskStatus) {
    for (JobStatusObserver observer : observers) {
      observer.onStatusUpdate(this, jobId, taskStatus);
    }
  }

  /**
   * Updates the {@link StoredJob} job.
   *
   * @return true if updateJob was successful and it changed the value, otherwise false.
   *     <p>Update only applies the change if the job already exists. You cannot use this method to
   *     add a new job that did not previously exist.
   */
  public synchronized boolean updateJob(long jobId, StoredJob job) {
    AtomicBoolean updated = new AtomicBoolean(false);
    jobsMap.computeIfPresent(
        jobId,
        (id, oldJob) -> {
          if (!oldJob.equals(job)) {
            updated.set(true);
            changed.set(true);
          }
          if (oldJob.getWorkerId() != job.getWorkerId()) {
            logger.debug(
                "update worker id in job",
                StructuredLogging.jobGroupId(storedJobGroup.getJobGroup().getJobGroupId()),
                StructuredLogging.jobId(jobId),
                StructuredLogging.fromId(oldJob.getWorkerId()),
                StructuredLogging.toId(job.getWorkerId()));
          }
          if (oldJob.getState() != job.getState()) {
            logger.debug(
                "update job state in job",
                StructuredLogging.jobGroupId(storedJobGroup.getJobGroup().getJobGroupId()),
                StructuredLogging.jobId(jobId),
                StructuredLogging.fromState(oldJob.getState().toString()),
                StructuredLogging.toState(job.getState().toString()));
          }
          return job;
        });
    return updated.get();
  }

  /**
   * Adds a new {@link StoredJob} to this job group.
   *
   * <p>Used by {@link LagMitigationRebalancer} to inject:
   *
   * <ul>
   *   <li>A replacement main job (after the old one is removed) that seeks to the latest offset.
   *   <li>A bounded catch-up job that replays the lagging offset range.
   * </ul>
   *
   * @return true if the job was added; false if a job with the same job ID already exists.
   */
  public synchronized boolean addJob(StoredJob job) {
    long jobId = job.getJob().getJobId();
    if (jobsMap.containsKey(jobId)) {
      return false;
    }
    jobsMap.put(jobId, job);
    changed.set(true);
    return true;
  }

  /**
   * Removes an existing {@link StoredJob} from this job group by job ID.
   *
   * <p>Used by {@link LagMitigationRebalancer} to evict the lagging main job before replacing it
   * with an updated version that seeks to the latest offset.
   *
   * @param jobId the ID of the job to remove
   * @return the removed job, or empty if no job with that ID was present
   */
  public synchronized java.util.Optional<StoredJob> removeJob(long jobId) {
    StoredJob removed = jobsMap.remove(jobId);
    if (removed != null) {
      changed.set(true);
      return java.util.Optional.of(removed);
    }
    return java.util.Optional.empty();
  }

  /**
   * Updates the Job Group State.
   *
   * @return true if update was successful and value was changed, otherwise false.
   */
  public synchronized boolean updateJobGroupState(JobState jobGroupState) {
    JobState oldState = storedJobGroup.getState();
    if (oldState == jobGroupState) {
      return false;
    }
    if (oldState != jobGroupState) {
      logger.info(
          "update job group state",
          StructuredLogging.fromState(oldState.toString()),
          StructuredLogging.toState(jobGroupState.toString()),
          StructuredLogging.jobGroupId(storedJobGroup.getJobGroup().getJobGroupId()));
    }
    storedJobGroup = storedJobGroup.toBuilder().setState(jobGroupState).build();
    changed.set(true);
    return true;
  }

  /**
   * Updates scale of the job group.
   *
   * @param newScale the new scale
   * @return the boolean
   */
  public synchronized boolean updateScale(double newScale, Throughput throughput) {
    if (storedJobGroup.hasScaleStatus() && newScale == storedJobGroup.getScaleStatus().getScale()) {
      return false;
    }
    ScaleStatus scaleStatus =
        ScaleStatus.newBuilder()
            .setScale(newScale)
            .setTotalMessagesPerSec(throughput.getMessagesPerSecond())
            .setTotalBytesPerSec(throughput.getBytesPerSecond())
            .build();
    storedJobGroup = StoredJobGroup.newBuilder(storedJobGroup).setScaleStatus(scaleStatus).build();
    changed.set(true);
    return true;
  }

  /** Returns a new StoredJobGroup that represents the (mutated) computation. */
  public Versioned<StoredJobGroup> toStoredJobGroup() {
    StoredJobGroup.Builder builder = storedJobGroup.toBuilder();
    builder.clearJobs();
    builder.addAllJobs(jobsMap.values());
    return Versioned.from(builder.build(), version);
  }

  /**
   * Returns a predicate that filters RebalancingJobGroup for job group state that matches the
   * provided input set.
   */
  public static Predicate<RebalancingJobGroup> filterByJobGroupState(final Set<JobState> states) {
    return rebalancingJobGroup -> states.contains(rebalancingJobGroup.getJobGroupState());
  }
}

