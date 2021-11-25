package org.apache.druid.client;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;

/**
 * !ndm
 * his节点准备线程操作集群集合逻辑
 * @author Chen768959
 * @date 2021/11/24
 */
public class DistributeMergeManager {
  private static DistributeMergeManager distributeMergeManager = new DistributeMergeManager();

  private DistributeMergeManager(){}

  /**
   * 开始聚合操作
   * @param query 此次查询请求对象
   * @param sequence 当前节点所有待查询分片的agg k-v list结果
   * @author Chen768959
   * @date 2021/11/24 下午 3:18
   * @return void
   */
  public void startMerge(Query query, Sequence sequence){

  }

  public static DistributeMergeManager getInstance() {
    return distributeMergeManager;
  }
}
