/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.druid.client.indexing.IndexingService;
import io.druid.discovery.DruidLeaderClient;
import io.druid.discovery.DruidLeaderSelector;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.Task;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.http.client.response.FullResponseHolder;
import io.druid.metadata.EntryExistsException;
import io.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Implements an in-heap TaskStorage facility, with no persistence across restarts. This class is not
 * thread safe.
 */
public class HeapMemoryTaskStorage implements TaskStorage
{
  private final TaskStorageConfig config;

  private final ReentrantLock giant = new ReentrantLock();
  private final AtomicReference<Map<String, TaskStuff>> tasks = new AtomicReference<>(Maps.newHashMap());
  private final AtomicReference<Multimap<String, TaskLock>> taskLocks = new AtomicReference<>(HashMultimap.create());
  private final Multimap<String, TaskAction> taskActions = ArrayListMultimap.create();
  private final DruidLeaderClient overlordLeaderClient;
  private final DruidLeaderSelector overlordLeaderSelector;
  private final ObjectMapper jsonMapper;

  private final ScheduledExecutorService standbySyncManagerExec = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
          .setDaemon(false)
          .setNameFormat("HeapMemoryTaskStorage-Standby-Sync").build()
  );

  private static final Logger log = new Logger(HeapMemoryTaskStorage.class);

  @Inject
  public HeapMemoryTaskStorage(
      TaskStorageConfig config,
      @IndexingService DruidLeaderSelector leaderSelector,
      @IndexingService DruidLeaderClient leaderClient,
      ObjectMapper jsonMapper

  )
  {
    this.config = config;
    this.overlordLeaderClient = leaderClient;
    this.overlordLeaderSelector = leaderSelector;
    this.jsonMapper = jsonMapper;
  }

  @LifecycleStart
  public void start()
  {
    ScheduledExecutors.scheduleAtFixedRate(
        standbySyncManagerExec,
        new Period("PT10S").toStandardDuration(), // hardcode for now
        () -> {
          if (this.overlordLeaderSelector.isLeader()) {
            log.debug("I am leader, I don't need to sync with others");
            return ScheduledExecutors.Signal.REPEAT;
          }
          try {
            syncFromLeader();
          }
          catch (Exception e) {
            log.error(e, "Failed to sync with leader");
          }
          return ScheduledExecutors.Signal.REPEAT;
        }
    );
  }

  public void syncFromLeader() throws Exception
  {
    boolean fallback = false;
    TaskStorageDataHolder taskStorageData = null;
    try {
      FullResponseHolder fullResponseHolder;
      log.debug("Sync TaskStorageWithLeader");
      fullResponseHolder = overlordLeaderClient.go(
          overlordLeaderClient.makeRequest(HttpMethod.GET, "/druid/indexer/v1/internal/taskStorage"));
      if (fullResponseHolder.getStatus().getCode() / 100 == 2) {
        log.debug("TaskStorage interval API works");
        final TaskStorageDataHolder data = jsonMapper.readValue(
            fullResponseHolder.getContent(),
            TaskStorageDataHolder.class
        );
        taskStorageData = data;
      }

    }
    catch (IOException | ChannelException e) {
      log.error(e, "Can't sync with internal api, trying public api");
      fallback = true;
    }

    if (fallback) {
    }

    if (taskStorageData != null) {
      Map<String, TaskStuff> newTasks = Maps.newHashMap();

      for (TaskStorageDataHolder.TaskInfoHolder holder : taskStorageData.getTasks()) {
        newTasks.put(
            holder.getTask().getId(),
            new TaskStuff(holder.getTask(), holder.getStatus(), holder.getCreatedDate(), holder.getDataSource())
        );
      }

      Multimap<String, TaskLock> newTaskLocks = taskStorageData.getTaskLockboxes();
      this.taskLocks.set(newTaskLocks);
      this.tasks.set(newTasks);
    } else {
      throw new Exception("Can't sync with leader");
    }

  }

  @LifecycleStop
  public void stop()
  {
    standbySyncManagerExec.shutdown();
  }

  @Override
  public void insert(Task task, TaskStatus status) throws EntryExistsException
  {
    giant.lock();

    try {
      Preconditions.checkNotNull(task, "task");
      Preconditions.checkNotNull(status, "status");
      Preconditions.checkArgument(
          task.getId().equals(status.getId()),
          "Task/Status ID mismatch[%s/%s]",
          task.getId(),
          status.getId()
      );

      if (tasks.get().containsKey(task.getId())) {
        throw new EntryExistsException(task.getId());
      }

      log.info("Inserting task %s with status: %s", task.getId(), status);
      tasks.get().put(task.getId(), new TaskStuff(task, status, DateTimes.nowUtc(), task.getDataSource()));
    }
    finally {
      giant.unlock();
    }
  }

  @Override
  public Optional<Task> getTask(String taskid)
  {
    giant.lock();

    try {
      Preconditions.checkNotNull(taskid, "taskid");
      if (tasks.get().containsKey(taskid)) {
        return Optional.of(tasks.get().get(taskid).getTask());
      } else {
        return Optional.absent();
      }
    }
    finally {
      giant.unlock();
    }
  }

  @Override
  public void setStatus(TaskStatus status)
  {
    giant.lock();

    try {
      Preconditions.checkNotNull(status, "status");

      final String taskid = status.getId();
      Preconditions.checkState(tasks.get().containsKey(taskid), "Task ID must already be present: %s", taskid);
      Preconditions.checkState(
          tasks.get().get(taskid).getStatus().isRunnable(),
          "Task status must be runnable: %s",
          taskid
      );
      log.info("Updating task %s to status: %s", taskid, status);
      tasks.get().put(taskid, tasks.get().get(taskid).withStatus(status));
    }
    finally {
      giant.unlock();
    }
  }

  @Override
  public Optional<TaskStatus> getStatus(String taskid)
  {
    giant.lock();

    try {
      Preconditions.checkNotNull(taskid, "taskid");
      if (tasks.get().containsKey(taskid)) {
        return Optional.of(tasks.get().get(taskid).getStatus());
      } else {
        return Optional.absent();
      }
    }
    finally {
      giant.unlock();
    }
  }


  private void cleanOldTasks()
  {
    long now = System.currentTimeMillis();
    long twoDaysInMillis = 1000 * 60 * 60 * 12;
    List<TaskStuff> oldTasks = tasks.get().values()
                                    .stream()
                                    .filter(taskStuff -> taskStuff.getStatus().isComplete()
                                                         && now
                                                            - taskStuff.getCreatedDate()
                                                                       .getMillis()
                                                            - taskStuff.getStatus().getDuration()
                                                            > twoDaysInMillis)
                                    .collect(Collectors.toList());
    int count = 0;
    for (TaskStuff t : oldTasks) {
      count++;
      tasks.get().remove(t.task.getId());
    }
    log.info("Cleaned %d tasks", count);
  }

  @Override
  public List<Task> getActiveTasks()
  {
    giant.lock();

    try {
      cleanOldTasks();
      final ImmutableList.Builder<Task> listBuilder = ImmutableList.builder();
      for (final TaskStuff taskStuff : tasks.get().values()) {
        if (taskStuff.getStatus().isRunnable()) {
          listBuilder.add(taskStuff.getTask());
        }
      }
      return listBuilder.build();
    }
    finally {
      giant.unlock();
    }
  }

  @Override
  public List<TaskStatus> getRecentlyFinishedTaskStatuses(@Nullable Integer maxTaskStatuses)
  {
    giant.lock();

    try {
      final Ordering<TaskStuff> createdDateDesc = new Ordering<TaskStuff>()
      {
        @Override
        public int compare(TaskStuff a, TaskStuff b)
        {
          return a.getCreatedDate().compareTo(b.getCreatedDate());
        }
      }.reverse();

      return maxTaskStatuses == null ?
             getRecentlyFinishedTaskStatusesSince(
                 System.currentTimeMillis() - config.getRecentlyFinishedThreshold().getMillis(),
                 createdDateDesc
             ) :
             getNRecentlyFinishedTaskStatuses(maxTaskStatuses, createdDateDesc);
    }
    finally {
      giant.unlock();
    }
  }

  private List<TaskStatus> getRecentlyFinishedTaskStatusesSince(long start, Ordering<TaskStuff> createdDateDesc)
  {
    giant.lock();

    try {
      return createdDateDesc
          .sortedCopy(tasks.get().values())
          .stream()
          .filter(taskStuff -> taskStuff.getStatus().isComplete() && taskStuff.getCreatedDate().getMillis() > start)
          .map(TaskStuff::getStatus)
          .collect(Collectors.toList());
    }
    finally {
      giant.unlock();
    }
  }

  private List<TaskStatus> getNRecentlyFinishedTaskStatuses(int n, Ordering<TaskStuff> createdDateDesc)
  {
    giant.lock();

    try {
      return createdDateDesc.sortedCopy(tasks.get().values())
                            .stream()
                            .limit(n)
                            .map(TaskStuff::getStatus)
                            .collect(Collectors.toList());
    }
    finally {
      giant.unlock();
    }
  }

  @Nullable
  @Override
  public Pair<DateTime, String> getCreatedDateTimeAndDataSource(String taskId)
  {
    giant.lock();

    try {
      final TaskStuff taskStuff = tasks.get().get(taskId);
      return taskStuff == null ? null : Pair.of(taskStuff.getCreatedDate(), taskStuff.getDataSource());
    }
    finally {
      giant.unlock();
    }
  }

  @Override
  public void addLock(final String taskid, final TaskLock taskLock)
  {
    giant.lock();

    try {
      Preconditions.checkNotNull(taskid, "taskid");
      Preconditions.checkNotNull(taskLock, "taskLock");
      taskLocks.get().put(taskid, taskLock);
    }
    finally {
      giant.unlock();
    }
  }

  @Override
  public void replaceLock(String taskid, TaskLock oldLock, TaskLock newLock)
  {
    giant.lock();

    try {
      Preconditions.checkNotNull(taskid, "taskid");
      Preconditions.checkNotNull(oldLock, "oldLock");
      Preconditions.checkNotNull(newLock, "newLock");

      if (!taskLocks.get().remove(taskid, oldLock)) {
        log.warn("taskLock[%s] for replacement is not found for task[%s]", oldLock, taskid);
      }

      taskLocks.get().put(taskid, newLock);
    }
    finally {
      giant.unlock();
    }
  }

  @Override
  public void removeLock(final String taskid, final TaskLock taskLock)
  {
    giant.lock();

    try {
      Preconditions.checkNotNull(taskLock, "taskLock");
      taskLocks.get().remove(taskid, taskLock);
    }
    finally {
      giant.unlock();
    }
  }

  @Override
  public List<TaskLock> getLocks(final String taskid)
  {
    giant.lock();

    try {
      return ImmutableList.copyOf(taskLocks.get().get(taskid));
    }
    finally {
      giant.unlock();
    }
  }

  @Override
  public TaskStorageDataHolder getData()
  {
    List<TaskStorageDataHolder.TaskInfoHolder> taskInfoLst = ImmutableList.copyOf(Iterables.transform(
        tasks.get().values(),
        new Function<TaskStuff, TaskStorageDataHolder.TaskInfoHolder>()
        {

          @Nullable
          @Override
          public TaskStorageDataHolder.TaskInfoHolder apply(@Nullable TaskStuff input)
          {
            return new TaskStorageDataHolder.TaskInfoHolder(
                input.getTask(),
                input.getStatus(),
                input.getCreatedDate(),
                input.getDataSource()
            );
          }
        }
    ));
    return new TaskStorageDataHolder(taskInfoLst, this.taskLocks.get());
  }

  @Override
  public <T> void addAuditLog(Task task, TaskAction<T> taskAction)
  {
    giant.lock();

    try {
      taskActions.put(task.getId(), taskAction);
    }
    finally {
      giant.unlock();
    }
  }

  @Override
  public List<TaskAction> getAuditLogs(String taskid)
  {
    giant.lock();

    try {
      return ImmutableList.copyOf(taskActions.get(taskid));
    }
    finally {
      giant.unlock();
    }
  }

  private static class TaskStuff
  {
    final Task task;
    final TaskStatus status;
    final DateTime createdDate;
    final String dataSource;

    private TaskStuff(Task task, TaskStatus status, DateTime createdDate, String dataSource)
    {
      Preconditions.checkArgument(task.getId().equals(status.getId()));

      this.task = Preconditions.checkNotNull(task, "task");
      this.status = Preconditions.checkNotNull(status, "status");
      this.createdDate = Preconditions.checkNotNull(createdDate, "createdDate");
      this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    }

    public Task getTask()
    {
      return task;
    }

    public TaskStatus getStatus()
    {
      return status;
    }

    public DateTime getCreatedDate()
    {
      return createdDate;
    }

    public String getDataSource()
    {
      return dataSource;
    }

    private TaskStuff withStatus(TaskStatus _status)
    {
      return new TaskStuff(task, _status, createdDate, dataSource);
    }
  }
}
