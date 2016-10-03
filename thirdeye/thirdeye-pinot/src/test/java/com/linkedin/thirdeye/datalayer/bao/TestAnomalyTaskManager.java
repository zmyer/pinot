package com.linkedin.thirdeye.datalayer.bao;

import java.util.HashSet;
import java.util.List;

import java.util.Set;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConstants.MonitorType;
import com.linkedin.thirdeye.anomaly.monitor.MonitorTaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;

public class TestAnomalyTaskManager extends AbstractManagerTestBase {

  private Long anomalyTaskId1;
  private Long anomalyTaskId2;
  private Long anomalyJobId;
  private static final Set<TaskStatus> allowedOldTaskStatus = new HashSet<>();
  static  {
    allowedOldTaskStatus.add(TaskStatus.FAILED);
    allowedOldTaskStatus.add(TaskStatus.WAITING);
  }

  @Test
  public void testCreate() throws JsonProcessingException {
    JobDTO testAnomalyJobSpec = getTestJobSpec();
    anomalyJobId = jobDAO.save(testAnomalyJobSpec);
    anomalyTaskId1 = taskDAO.save(getTestTaskSpec(testAnomalyJobSpec));
    Assert.assertNotNull(anomalyTaskId1);
    anomalyTaskId2 = taskDAO.save(getTestTaskSpec(testAnomalyJobSpec));
    Assert.assertNotNull(anomalyTaskId2);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAll() {
    List<TaskDTO> anomalyTasks = taskDAO.findAll();
    Assert.assertEquals(anomalyTasks.size(), 2);
  }

  @Test(dependsOnMethods = { "testFindAll" })
  public void testUpdateStatusAndWorkerId() {
    TaskStatus newStatus = TaskStatus.RUNNING;
    Long workerId = 1L;

    boolean status =
        taskDAO.updateStatusAndWorkerId(workerId, anomalyTaskId1, allowedOldTaskStatus, newStatus);
    TaskDTO anomalyTask = taskDAO.findById(anomalyTaskId1);
    Assert.assertTrue(status);
    Assert.assertEquals(anomalyTask.getStatus(), newStatus);
    Assert.assertEquals(anomalyTask.getWorkerId(), workerId);
  }

  @Test(dependsOnMethods = {"testUpdateStatusAndWorkerId"})
  public void testFindByStatusOrderByCreationTimeAsc() {
    List<TaskDTO> anomalyTasks =
        taskDAO.findByStatusOrderByCreateTime(TaskStatus.WAITING, Integer.MAX_VALUE, true);
    Assert.assertEquals(anomalyTasks.size(), 1);
  }

  @Test(dependsOnMethods = {"testFindByStatusOrderByCreationTimeAsc"})
  public void testUpdateStatusAndTaskEndTime() {
    TaskStatus oldStatus = TaskStatus.RUNNING;
    TaskStatus newStatus = TaskStatus.COMPLETED;
    long taskEndTime = System.currentTimeMillis();
    taskDAO.updateStatusAndTaskEndTime(anomalyTaskId1, oldStatus, newStatus, taskEndTime);
    TaskDTO anomalyTask = taskDAO.findById(anomalyTaskId1);
    Assert.assertEquals(anomalyTask.getStatus(), newStatus);
    Assert.assertEquals(anomalyTask.getEndTime(), taskEndTime);
  }

  @Test(dependsOnMethods = {"testUpdateStatusAndTaskEndTime"})
  public void testFindByJobIdStatusNotIn() {
    TaskStatus status = TaskStatus.COMPLETED;
    List<TaskDTO> anomalyTaskSpecs = taskDAO.findByJobIdStatusNotIn(anomalyJobId, status);
    Assert.assertEquals(anomalyTaskSpecs.size(), 1);
  }

  @Test(dependsOnMethods = {"testFindByJobIdStatusNotIn"})
  public void testDeleteRecordOlderThanDaysWithStatus() {
    TaskStatus status = TaskStatus.COMPLETED;
    int numRecordsDeleted = taskDAO.deleteRecordsOlderThanDaysWithStatus(0, status);
    Assert.assertEquals(numRecordsDeleted, 1);
  }

  TaskDTO getTestTaskSpec(JobDTO anomalyJobSpec) throws JsonProcessingException {
    TaskDTO jobSpec = new TaskDTO();
    jobSpec.setJobName("Test_Anomaly_Task");
    jobSpec.setStatus(TaskStatus.WAITING);
    jobSpec.setTaskType(TaskType.MONITOR);
    jobSpec.setStartTime(new DateTime().minusDays(20).getMillis());
    jobSpec.setEndTime(new DateTime().minusDays(10).getMillis());
    jobSpec.setTaskInfo(new ObjectMapper().writeValueAsString(getTestMonitorTaskInfo()));
    jobSpec.setJob(anomalyJobSpec);
    return jobSpec;
  }

  static MonitorTaskInfo getTestMonitorTaskInfo() {
    MonitorTaskInfo taskInfo = new MonitorTaskInfo();
    taskInfo.setJobExecutionId(1L);
    taskInfo.setMonitorType(MonitorType.UPDATE);
    return taskInfo;
  }
}
