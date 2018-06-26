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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import org.joda.time.DateTime;

import java.util.List;

/**
 * A jackson friendly holder for TaskStorageData
 */
public final class TaskStorageDataHolder
{
  private final List<TaskInfoHolder> tasks;
  private final List<TaskLock> taskLocks;

  @JsonCreator
  public TaskStorageDataHolder(
      @JsonProperty("tasks") List<TaskInfoHolder> tasks,
      @JsonProperty("taskLockboxes") List<TaskLock> taskLocks
  )
  {
    this.taskLocks = taskLocks;
    this.tasks = tasks;
  }

  @JsonProperty("tasks")
  public List<TaskInfoHolder> getTasks()
  {
    return this.tasks;
  }

  @JsonProperty("taskLocks")
  public List<TaskLock> getTaskLockboxes()
  {
    return this.taskLocks;
  }

  public static class TaskInfoHolder
  {
    final Task task;
    final TaskStatus status;
    final DateTime createdDate;
    final String dataSource;

    @JsonCreator
    public TaskInfoHolder(
        @JsonProperty("task") Task task,
        @JsonProperty("status") TaskStatus status,
        @JsonProperty("createdDate") DateTime createdDate,
        @JsonProperty("dataSource") String dataSource
    )
    {
      Preconditions.checkArgument(task.getId().equals(status.getId()));

      this.task = Preconditions.checkNotNull(task, "task");
      this.status = Preconditions.checkNotNull(status, "status");
      this.createdDate = Preconditions.checkNotNull(createdDate, "createdDate");
      this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    }

    @JsonProperty("task")
    public Task getTask()
    {
      return task;
    }

    @JsonProperty("status")
    public TaskStatus getStatus()
    {
      return status;
    }

    @JsonProperty("createdDate")
    public DateTime getCreatedDate()
    {
      return createdDate;
    }

    @JsonProperty("dataSource")
    public String getDataSource()
    {
      return dataSource;
    }
  }
}

