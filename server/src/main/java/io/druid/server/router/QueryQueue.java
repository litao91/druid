package io.druid.server.router;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.EntryExistsException;
import io.druid.query.Query;
import io.druid.server.router.setup.QueryProxyBehaviorConfig;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class QueryQueue
{
  private final Supplier<QueryProxyBehaviorConfig> proxyConfigRef;
  private final LinkedList<Query> queries = Lists.newLinkedList();
  private final Map<String, QueryStuff> queryMap = Maps.newHashMap();

  private final ReentrantLock lock;
  private final Condition notEmpty;

  private static final Logger logger = new Logger(QueryQueue.class);


  @Inject
  public QueryQueue(
      final Supplier<QueryProxyBehaviorConfig> proxyConfigRef
  )
  {
    this.proxyConfigRef = proxyConfigRef;
     lock = new ReentrantLock(true);
     notEmpty = lock.newCondition();
  }

  public boolean add(final Query query, HttpServletRequest request, HttpServletResponse response) throws EntryExistsException
  {
    lock.lock();
    try {
      Preconditions.checkNotNull(query, "query");
      if (queries.size() > proxyConfigRef.get().getQueryQueueSize()) {
        return false;
      }
      if (queryMap.containsKey(query.getId())) {
        throw new EntryExistsException(query.getId());
      }
      logger.info("Adding query %s", query.getId());
      queryMap.put(query.getId(),
          new QueryStuff(
              query,
              QueryStatus.running(query.getId()),
              DateTimes.nowUtc(),
              request,
              response));
      queries.add(query);
      notEmpty.signal();
      return true;
    } finally {
      lock.unlock();
    }
  }

  public QueryStuff pollNextToRun() throws InterruptedException
  {
    lock.lockInterruptibly();
    try {
      while(queries.isEmpty())
        notEmpty.await();
      Query q = queries.remove();
      return queryMap.remove(q.getId());
    } finally {
      lock.unlock();
    }
  }

  static class QueryStuff
  {
    final Query query;
    final QueryStatus status;
    final DateTime createdDate;
    final HttpServletRequest request;
    final HttpServletResponse response;

    private QueryStuff(
        Query query,
        QueryStatus status,
        DateTime createdDate,
        HttpServletRequest request,
        HttpServletResponse response
    )
    {
      Preconditions.checkArgument(query.getId().equals(status.getId()));
      this.query = query;
      this.status = status;
      this.createdDate = createdDate;
      this.request = request;
      this.response = response;
    }
  }


}
