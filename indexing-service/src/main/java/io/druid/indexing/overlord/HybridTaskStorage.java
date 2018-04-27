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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.task.Task;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.metadata.EntryExistsException;
import io.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The hybird task storage takes the advantages of both HeapMemoryTaskStorage and MetadataTaskStorage.
 * For performance consideration, we first write the task info to the HeampMemoryStorage and then
 * asynchronizally sync the data to MetadataTaskStorage.
 * <p>
 * When we need to read data, we first read data from HeapTaskStorage, and then MetadataTaskStorage.
 * <p>
 * This implementation may lead to some consistency problem, as the data in HeapTaskStorage and MetadataTaskStorage
 * may diverse, but it's not a huge problem as the Overlord generally don't need a very high consistency level.
 * We can bare some data loss and data discrepency.
 */
public class HybridTaskStorage implements TaskStorage
{
  private final HeapMemoryTaskStorage heapMemoryTaskStorage;
  private final MetadataTaskStorage metadataTaskStorage;
  private final ExecutorService metadataExec = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
          .setDaemon(false)
          .setNameFormat("MeatadataTaskStorage-Executor").build()
  );
  private static final Logger log = new Logger(HybridTaskStorage.class);

  @Inject
  public HybridTaskStorage(HeapMemoryTaskStorage heapMemoryTaskStorage, MetadataTaskStorage metadataTaskStorage)
  {
    this.heapMemoryTaskStorage = heapMemoryTaskStorage;
    this.metadataTaskStorage = metadataTaskStorage;
  }

  @LifecycleStart
  public void start()
  {
    this.metadataTaskStorage.start();
    // Initialize the heap memory
    for (final Task task : metadataTaskStorage.getActiveTasks()) {
      // sync active tasks
      Optional<TaskStatus> status = this.metadataTaskStorage.getStatus(task.getId());
      if (status.isPresent()) {
        try {
          this.heapMemoryTaskStorage.insert(task, status.get());
        }
        catch (EntryExistsException e) {
          log.error(e, "Something bad happned during sync data to heap!");
          continue;
        }
      } else {
        continue;
      }
      // sync task locks
      for (final TaskLock taskLock : metadataTaskStorage.getLocks(task.getId())) {
        heapMemoryTaskStorage.addLock(task.getId(), taskLock);
      }
      // sync audit logs
      for (final TaskAction action : metadataTaskStorage.getAuditLogs(task.getId())) {
        this.heapMemoryTaskStorage.addAuditLog(task, action);
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    this.metadataTaskStorage.stop();
  }


  @Override
  public void insert(Task task, TaskStatus status) throws EntryExistsException
  {
    this.heapMemoryTaskStorage.insert(task, status);
    metadataExec.submit(() -> {
      try {
        this.metadataTaskStorage.insert(task, status);
      }
      catch (EntryExistsException e) {
        log.error(e, "Something bad happened, this should not be possible");
      }
    });
  }

  @Override
  public Optional<Task> getTask(String taskid)
  {
    Optional<Task> task = heapMemoryTaskStorage.getTask(taskid);
    if (task.isPresent()) {
      return task;
    } else {
      return metadataTaskStorage.getTask(taskid);
    }
  }

  @Override
  public Optional<TaskStatus> getStatus(String taskid)
  {
    Optional<TaskStatus> status = heapMemoryTaskStorage.getStatus(taskid);
    if (status.isPresent()) {
      return status;
    } else {
      return metadataTaskStorage.getStatus(taskid);
    }
  }

  @Override
  public void setStatus(TaskStatus status)
  {
    heapMemoryTaskStorage.setStatus(status);
    metadataExec.execute(() -> metadataTaskStorage.setStatus(status));
  }

  @Override
  public void addLock(String taskid, TaskLock taskLock)
  {
    heapMemoryTaskStorage.addLock(taskid, taskLock);
    metadataExec.execute(() -> metadataTaskStorage.addLock(taskid, taskLock));

  }

  @Override
  public void replaceLock(String taskid, TaskLock oldLock, TaskLock newLock)
  {
    heapMemoryTaskStorage.replaceLock(taskid, oldLock, newLock);
    metadataExec.execute(() -> metadataTaskStorage.replaceLock(taskid, oldLock, newLock));
  }

  @Override
  public void removeLock(String taskid, TaskLock taskLock)
  {
    heapMemoryTaskStorage.removeLock(taskid, taskLock);
    metadataExec.execute(() -> metadataTaskStorage.removeLock(taskid, taskLock));
  }


  @Override
  public <T> void addAuditLog(Task task, TaskAction<T> taskAction)
  {
    heapMemoryTaskStorage.addAuditLog(task, taskAction);
    metadataExec.execute(() -> metadataTaskStorage.addAuditLog(task, taskAction));
  }

  @Override
  public List<TaskAction> getAuditLogs(String taskid)
  {
    List<TaskAction> actions = heapMemoryTaskStorage.getAuditLogs(taskid);
    if (actions.size() == 0) {
      return metadataTaskStorage.getAuditLogs(taskid);
    } else {
      return actions;
    }
  }

  @Override
  public List<Task> getActiveTasks()
  {
    return heapMemoryTaskStorage.getActiveTasks();
  }

  @Override
  public List<TaskStatus> getRecentlyFinishedTaskStatuses(@Nullable Integer maxTaskStatuses)
  {
    return heapMemoryTaskStorage.getRecentlyFinishedTaskStatuses(maxTaskStatuses);
  }

  @Nullable
  @Override
  public Pair<DateTime, String> getCreatedDateTimeAndDataSource(String taskId)
  {
    Pair<DateTime, String> r = heapMemoryTaskStorage.getCreatedDateTimeAndDataSource(taskId);
    if (r != null) {
      return r;
    } else {
      return metadataTaskStorage.getCreatedDateTimeAndDataSource(taskId);
    }
  }

  @Override
  public List<TaskLock> getLocks(String taskid)
  {
    return heapMemoryTaskStorage.getLocks(taskid);
  }
}
