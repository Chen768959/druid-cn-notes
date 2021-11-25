package org.apache.druid.client;

import org.apache.druid.java.util.common.guava.Sequence;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * !ndm
 * 负责broker暂停等待，
 * his节点存放结果，broker节点获取结果
 * @author Chen768959
 * @date 2021/11/23
 */
public class DistributeResultManager {
  private static DistributeResultManager distributeResultManager = new DistributeResultManager();

  private DistributeResultManager(){}

  // 所有等待中的查询线程
  private Map<String, Object> queryThreadsWaitMap = new ConcurrentHashMap<>();

  // 所有查询线程的最终结果
  private Map<String, Sequence> queryThreadsResultMap = new ConcurrentHashMap<>();

  public static DistributeResultManager getInstance() {
    return distributeResultManager;
  }

  /**
   * 唤醒指定查询线程
   * @param queryId
   * @author Chen768959
   * @date 2021/11/23 下午 7:14
   * @return void
   */
  boolean notifyThread(String queryId){
    Object o = queryThreadsWaitMap.get(queryId);
    if (o==null){
      return false;
    }
    o.notify();
    queryThreadsWaitMap.remove(queryId);
    return true;
  }

  void waitThread(String queryId, Object key) throws InterruptedException {
    key.wait();
    queryThreadsWaitMap.put(queryId, key);
  }

  void setDistributeMergeResult(String queryId, Sequence res){
    queryThreadsResultMap.put(queryId, res);
  }

  Sequence getDistributeMergeResult(String queryId){
    Sequence sequence = queryThreadsResultMap.get(queryId);
    queryThreadsResultMap.remove(queryId);
    return sequence;
  }
}
