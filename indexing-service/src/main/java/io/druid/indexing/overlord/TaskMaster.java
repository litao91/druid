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
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.common.guava.DSuppliers;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.client.indexing.IndexingService;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.discovery.DruidLeaderSelector;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.indexing.overlord.config.TaskQueueConfig;
import io.druid.indexing.overlord.helpers.OverlordHelperManager;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.DruidNode;
import io.druid.server.coordinator.CoordinatorOverlordServiceConfig;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Encapsulates the indexer leadership lifecycle.
 */
public class TaskMaster
{
  private final DruidLeaderSelector overlordLeaderSelector;
  private final DruidLeaderSelector.Listener leadershipListener;

  private final ReentrantLock giant = new ReentrantLock(true);
  private final TaskActionClientFactory taskActionClientFactory;
  private final SupervisorManager supervisorManager;

  private volatile TaskRunner taskRunner;
  private volatile TaskQueue taskQueue;

  private static final EmittingLogger log = new EmittingLogger(TaskMaster.class);


  private final AtomicReference<Boolean> isLeaderRef = new AtomicReference<>(false);
  private final Supplier<Boolean> isLeaderSupplier = DSuppliers.of(isLeaderRef);

  private final TaskLockbox taskLockbox;
  private final TaskQueueConfig taskQueueConfig;
  private final TaskRunnerFactory runnerFactory;
  private final TaskStorage taskStorage;
  private final ServiceEmitter emitter;
  private final OverlordHelperManager overlordHelperManager;

  @Inject
  public TaskMaster(
      final TaskQueueConfig taskQueueConfig,
      final TaskLockbox taskLockbox,
      final TaskStorage taskStorage,
      final TaskActionClientFactory taskActionClientFactory,
      @Self final DruidNode selfNode,
      final TaskRunnerFactory runnerFactory,
      final ServiceAnnouncer serviceAnnouncer,
      final CoordinatorOverlordServiceConfig coordinatorOverlordServiceConfig,
      final ServiceEmitter emitter,
      final SupervisorManager supervisorManager,
      final OverlordHelperManager overlordHelperManager,
      @IndexingService final DruidLeaderSelector overlordLeaderSelector
  )
  {
    this.supervisorManager = supervisorManager;
    this.taskActionClientFactory = taskActionClientFactory;

    this.overlordLeaderSelector = overlordLeaderSelector;

    final DruidNode node = coordinatorOverlordServiceConfig.getOverlordService() == null ? selfNode :
                           selfNode.withService(coordinatorOverlordServiceConfig.getOverlordService());

    this.taskLockbox = taskLockbox;
    this.taskQueueConfig = taskQueueConfig;
    this.runnerFactory = runnerFactory;
    this.taskStorage = taskStorage;
    this.emitter = emitter;
    this.overlordHelperManager = overlordHelperManager;

    this.leadershipListener = new DruidLeaderSelector.Listener()
    {
      @Override
      public void becomeLeader()
      {
        // I AM THE MASTER OF THE UNIVERSE.
        log.info("By the power of Grayskull, I have the power!");

        giant.lock();
        try {
          isLeaderRef.set(true);
          serviceAnnouncer.announce(node);
        }
        finally {
          giant.unlock();
        }
      }

      @Override
      public void stopBeingLeader()
      {
        giant.lock();
        try {
          serviceAnnouncer.unannounce(node);
          isLeaderRef.set(false);
        }
        finally {
          giant.unlock();
        }
      }
    };
  }

  /**
   * Starts waiting for leadership. Should only be called once throughout the life of the program.
   */
  @LifecycleStart
  public void start()
  {
    giant.lock();

    try {
      overlordLeaderSelector.registerListener(leadershipListener);
      taskLockbox.syncFromStorage();
      taskRunner = runnerFactory.build();
      taskQueue = new TaskQueue(
          taskQueueConfig,
          taskStorage,
          taskRunner,
          taskActionClientFactory,
          taskLockbox,
          emitter,
          isLeaderSupplier
      );


      taskRunner.start();
      taskQueue.start();
      supervisorManager.start();
      overlordHelperManager.start();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Stops forever (not just this particular leadership session). Should only be called once throughout the life of
   * the program.
   */
  @LifecycleStop
  public void stop()
  {
    giant.lock();

    try {
      overlordLeaderSelector.unregisterListener();
      taskRunner.stop();
      taskQueue.stop();
      supervisorManager.stop();
      overlordHelperManager.stop();
    }
    finally {
      giant.unlock();
    }
  }

  public boolean isLeader()
  {
    return overlordLeaderSelector.isLeader();
  }

  public String getCurrentLeader()
  {
    return overlordLeaderSelector.getCurrentLeader();
  }

  public Optional<TaskRunner> getTaskRunner()
  {
    if (overlordLeaderSelector.isLeader()) {
      return Optional.of(taskRunner);
    } else {
      return Optional.absent();
    }
  }

  public Optional<TaskQueue> getTaskQueue()
  {
    if (overlordLeaderSelector.isLeader()) {
      return Optional.of(taskQueue);
    } else {
      return Optional.absent();
    }
  }

  public Optional<TaskActionClient> getTaskActionClient(Task task)
  {
    if (overlordLeaderSelector.isLeader()) {
      return Optional.of(taskActionClientFactory.create(task));
    } else {
      return Optional.absent();
    }
  }

  public Optional<ScalingStats> getScalingStats()
  {
    if (overlordLeaderSelector.isLeader()) {
      return taskRunner.getScalingStats();
    } else {
      return Optional.absent();
    }
  }

  public Optional<SupervisorManager> getSupervisorManager()
  {
    if (overlordLeaderSelector.isLeader()) {
      return Optional.of(supervisorManager);
    } else {
      return Optional.absent();
    }
  }
}
