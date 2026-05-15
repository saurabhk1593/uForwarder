package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.uber.data.kafka.datatransfer.KafkaConsumerTaskStatus;

/**
 * Observer interface for job-status update events emitted by {@link RebalancingJobGroup}.
 *
 * <p>Implementations receive a callback each time a job's {@link KafkaConsumerTaskStatus} is
 * replayed through {@link RebalancingJobGroup#updateJobStatus}. This allows stateful components
 * such as {@link LagDetector} to maintain an incremental index instead of performing a full scan
 * every rebalancing cycle.
 *
 * <p>Implementations must be thread-safe. {@link RebalancingJobGroup} iterates observers under its
 * own synchronisation and passes values by value (proto objects are immutable), so implementations
 * may safely store the arguments.
 */
public interface JobStatusObserver {

  /**
   * Called when a job's consumer-task status has been (re-)evaluated for {@code group}.
   *
   * @param group the job group that owns the job
   * @param jobId the job whose status changed
   * @param taskStatus the latest {@link KafkaConsumerTaskStatus} reported by the worker
   */
  void onStatusUpdate(RebalancingJobGroup group, long jobId, KafkaConsumerTaskStatus taskStatus);
}


