package io.druid.server.router;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.indexer.TaskState;

public class QueryStatus
{
  public static QueryStatus running(String taskId)
  {
    return new QueryStatus(taskId, TaskState.RUNNING, -1);
  }

  public static QueryStatus success(String taskId)
  {
    return new QueryStatus(taskId, TaskState.SUCCESS, -1);
  }

  public static QueryStatus failure(String taskId)
  {
    return new QueryStatus(taskId, TaskState.FAILED, -1);
  }

  private final String id;
  private TaskState status;
  private final long duration;

  protected QueryStatus(
      @JsonProperty("id") String id,
      @JsonProperty("status") TaskState status,
      @JsonProperty("duration") long duration
  )
  {
    this.id = id;
    this.status = status;
    this.duration = duration;

    Preconditions.checkNotNull(id, "id");
    Preconditions.checkNotNull(status, "status");
  }

  @JsonProperty("id")
  public String getId()
  {
    return id;
  }

  @JsonProperty("status")
  public TaskState getStatusCode()
  {
    return status;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return duration;
  }
}
