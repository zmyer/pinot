/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.minion;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>PinotHelixTaskResourceManager</code> manages all the task resources in Pinot cluster.
 */
public class PinotHelixTaskResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixTaskResourceManager.class);

  // Do not change this because Helix uses the same separator
  public static final String TASK_NAME_SEPARATOR = "_";

  private static final String TASK_QUEUE_PREFIX = "TaskQueue" + TASK_NAME_SEPARATOR;
  private static final String TASK_PREFIX = "Task" + TASK_NAME_SEPARATOR;

  private final TaskDriver _taskDriver;

  public PinotHelixTaskResourceManager(@Nonnull TaskDriver taskDriver) {
    _taskDriver = taskDriver;
  }

  /**
   * Get all task types.
   *
   * @return Set of all task types
   */
  @Nonnull
  public synchronized Set<String> getTaskTypes() {
    Set<String> helixJobQueues = _taskDriver.getWorkflows().keySet();
    Set<String> taskTypes = new HashSet<>(helixJobQueues.size());
    for (String helixJobQueue : helixJobQueues) {
      taskTypes.add(getTaskType(helixJobQueue));
    }
    return taskTypes;
  }

  /**
   * Create a task queue for the given task type.
   *
   * @param taskType Task type
   */
  public synchronized void createTaskQueue(@Nonnull String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Creating task queue: {} for task type: {}", helixJobQueueName, taskType);

    // Set full parallelism
    JobQueue jobQueue = new JobQueue.Builder(helixJobQueueName).setWorkflowConfig(
        new WorkflowConfig.Builder().setParallelJobs(Integer.MAX_VALUE).build()).build();
    _taskDriver.createQueue(jobQueue);
  }

  /**
   * Stop the task queue for the given task type.
   *
   * @param taskType Task type
   */
  public synchronized void stopTaskQueue(@Nonnull String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Stopping task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.stop(helixJobQueueName);
  }

  /**
   * Resume the task queue for the given task type.
   *
   * @param taskType Task type
   */
  public synchronized void resumeTaskQueue(@Nonnull String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Resuming task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.resume(helixJobQueueName);
  }

  /**
   * Delete the task queue for the given task type.
   *
   * @param taskType Task type
   */
  public synchronized void deleteTaskQueue(@Nonnull String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Deleting task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.delete(helixJobQueueName);
  }

  /**
   * Get all task queues.
   *
   * @return Set of task queue names
   */
  public synchronized Set<String> getTaskQueues() {
    return _taskDriver.getWorkflows().keySet();
  }

  /**
   * Get the task queue state for the given task type.
   *
   * @param taskType Task type
   * @return Task queue state
   */
  public synchronized TaskState getTaskQueueState(@Nonnull String taskType) {
    return _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getWorkflowState();
  }

  /**
   * Submit a task to the Minion instances with the default tag.
   *
   * @param pinotTaskConfig Task config of the task to be submitted
   * @return Name of the submitted task
   */
  @Nonnull
  public synchronized String submitTask(@Nonnull PinotTaskConfig pinotTaskConfig) {
    return submitTask(pinotTaskConfig, CommonConstants.Minion.UNTAGGED_INSTANCE);
  }

  /**
   * Submit a task to the Minion instances with the given tag.
   *
   * @param pinotTaskConfig Task config of the task to be submitted
   * @param minionInstanceTag Tag of the Minion instances to submit the task to
   * @return Name of the submitted task
   */
  @Nonnull
  public synchronized String submitTask(@Nonnull PinotTaskConfig pinotTaskConfig, @Nonnull String minionInstanceTag) {
    String taskType = pinotTaskConfig.getTaskType();
    String taskName = TASK_PREFIX + taskType + TASK_NAME_SEPARATOR + System.nanoTime();
    LOGGER.info("Submitting task: {} of type: {} with config: {} to Minion instances with tag: {}", taskName, taskType,
        pinotTaskConfig, minionInstanceTag);
    JobConfig.Builder jobBuilder = new JobConfig.Builder().setInstanceGroupTag(minionInstanceTag)
        .addTaskConfigs(Collections.singletonList(pinotTaskConfig.toHelixTaskConfig(taskName)));
    _taskDriver.enqueueJob(getHelixJobQueueName(taskType), taskName, jobBuilder);
    return taskName;
  }

  /**
   * Get all tasks for the given task type.
   *
   * @param taskType Task type
   * @return Set of task names
   */
  @Nonnull
  public synchronized Set<String> getTasks(@Nonnull String taskType) {
    Set<String> helixJobs = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getJobStates().keySet();
    Set<String> tasks = new HashSet<>(helixJobs.size());
    for (String helixJobName : helixJobs) {
      tasks.add(getPinotTaskName(helixJobName));
    }
    return tasks;
  }

  /**
   * Get all task states for the given task type.
   *
   * @param taskType Task type
   * @return Map from task name to task state
   */
  @Nonnull
  public synchronized Map<String, TaskState> getTaskStates(@Nonnull String taskType) {
    Map<String, TaskState> helixJobStates =
        _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getJobStates();
    Map<String, TaskState> taskStates = new HashMap<>(helixJobStates.size());
    for (Map.Entry<String, TaskState> entry : helixJobStates.entrySet()) {
      taskStates.put(getPinotTaskName(entry.getKey()), entry.getValue());
    }
    return taskStates;
  }

  /**
   * Get the task state for the given task name.
   *
   * @param taskName Task name
   * @return Task state
   */
  public synchronized TaskState getTaskState(@Nonnull String taskName) {
    String taskType = getTaskType(taskName);
    return _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getJobState(getHelixJobName(taskName));
  }

  /**
   * Get the task config for the given task name.
   *
   * @param taskName Task name
   * @return Task config
   */
  @Nonnull
  public synchronized PinotTaskConfig getTaskConfig(@Nonnull String taskName) {
    return PinotTaskConfig.fromHelixTaskConfig(
        _taskDriver.getJobConfig(getHelixJobName(taskName)).getTaskConfig(taskName));
  }

  /**
   * Helper method to convert task type to Helix JobQueue name.
   * <p>E.g. DummyTask -> TaskQueue_DummyTask
   *
   * @param taskType Task type
   * @return Helix JobQueue name
   */
  @Nonnull
  protected static String getHelixJobQueueName(@Nonnull String taskType) {
    return TASK_QUEUE_PREFIX + taskType;
  }

  /**
   * Helper method to convert Pinot task name to Helix Job name with JobQueue prefix.
   * <p>E.g. Task_DummyTask_12345 -> TaskQueue_DummyTask_Task_DummyTask_12345
   *
   * @param pinotTaskName Pinot task name
   * @return helixJobName Helix Job name
   */
  @Nonnull
  private static String getHelixJobName(@Nonnull String pinotTaskName) {
    return getHelixJobQueueName(getTaskType(pinotTaskName)) + TASK_NAME_SEPARATOR + pinotTaskName;
  }

  /**
   * Helper method to convert Helix Job name with JobQueue prefix to Pinot task name.
   * <p>E.g. TaskQueue_DummyTask_Task_DummyTask_12345 -> Task_DummyTask_12345
   *
   * @param helixJobName Helix Job name
   * @return Pinot task name
   */
  @Nonnull
  private static String getPinotTaskName(@Nonnull String helixJobName) {
    return helixJobName.substring(TASK_QUEUE_PREFIX.length() + getTaskType(helixJobName).length() + 1);
  }

  /**
   * Helper method to extract task type from Pinot task name, Helix JobQueue name or Helix Job name.
   * <p>E.g. Task_DummyTask_12345 -> DummyTask (from Pinot task name)
   * <p>E.g. TaskQueue_DummyTask -> DummyTask (from Helix JobQueue name)
   * <p>E.g. TaskQueue_DummyTask_Task_DummyTask_12345 -> DummyTask (from Helix Job name)
   *
   * @param name Pinot task name, Helix JobQueue name or Helix Job name
   * @return Task type
   */
  @Nonnull
  private static String getTaskType(@Nonnull String name) {
    return name.split(TASK_NAME_SEPARATOR)[1];
  }
}
