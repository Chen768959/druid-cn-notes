package org.apache.druid.client;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.SegmentDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * !ndm
 * broker规划聚合节点
 *
 * @author Chen768959
 * @date 2021/11/24
 */
public class DistributeBrokerServer {
  private static DistributeBrokerServer distributeBrokerServer = new DistributeBrokerServer();

  // key:queryId，value:聚合计划包装类
  private final Map<String, DistributeQueryPlan> distributeQueryPlanMap = new ConcurrentHashMap<>();

  public static DistributeBrokerServer getInstance() {
    return distributeBrokerServer;
  }

  /**
   *
   * @param query 此次查询对象
   * @param segmentsByServer 涉及到的查询主机以及待查询segment分片信息
   * @param distributeHosts 所有参与聚合的主机ip
   * @author Chen768959
   * @date 2021/12/7 下午 2:37
   * @return void
   */
  public void initServer(Query query, SortedMap<DruidServer, List<SegmentDescriptor>> segmentsByServer, List<String> distributeHosts) {
    Map<String, AtomicInteger> hisHostsNeedRespCount = segmentsByServer.entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().getHost(),
            entry -> new AtomicInteger(entry.getValue().size())));

    DistributeQueryPlan distributeQueryPlan = new DistributeQueryPlan(distributeHosts, hisHostsNeedRespCount);
    distributeQueryPlanMap.put(query.getId(), distributeQueryPlan);
  }


  /**
   * broker->his：（broker记录每台his需要查询的seg数）然后通知所有his主机开始agg查询
   * his->broker：his查询完毕，并本机combine完毕后，通知broker并发送所有key，由broker确定所有key的去向（broker清零该his需要查询的seg数）
   * broker->his：
   */
  public Sequence processServer(Query query) {
    DistributeQueryPlan distributeQueryPlan = distributeQueryPlanMap.get(query.getId());

    // 判断agg阶段是否完成（根据每台his主机需要查询的seg数量是否为0为标准）
    aggContinue: while (true){
      for (AtomicInteger value : distributeQueryPlan.hisHostsNeedRespCount.values()) {
        if (value.get()!=0){
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          continue aggContinue;
        }
      }
      break;
    }

    // 此时所有分片查询都已完毕且都已告知broker（his节点本机combine结束后才会告知broker），broker已分配所有参与combine节点
    // 通过每台主机需要聚合的参数来判断combine阶段是否完毕
    combineContinue :while (true){
      for (AtomicInteger value : distributeQueryPlan.hostNeedMergeCount.values()) {
        if (value.get()==0){
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          continue combineContinue;
        }
      }
      break;
    }

    // 此时所有参与聚合的主机都在进行分布式combine，
    // his节点自身combine完毕后，会告知broker，broker就知道该his节点可以参与分布式聚合了，
    // 然后broker会分配该主机节点如何分发，以及后续什么节点需要传给该主机，
    // 此过程需要尽量均匀的把所有key分发到所有work主机上。
    // 当所有his节点都完毕后，broker就知道每台work主机所参与的combine，以及该节点何时combine完毕，（在每次work节点combine完一次，然后通知broker后，可检测出来）。

    // broker通知work后，work就继续进行final和本机final merge，
    //


    // 判断
    return null;
  }
  // 开始查询，掌握所有涉及的his主机，以及所有参与聚合的主机

  /**
   * 在combine阶段，接收agg+key，计算该value交由那台主机处理
   * @param query
   * @param fromHost 来源主机
   * @param aggNamesAndKeys 每个聚合器名与其包含的key
   * @author Chen768959
   * @date 2021/12/10 上午 11:20
   * @return java.util.List<org.apache.druid.client.DistributeHisPostClient.PostKeyToBrokerRes>
   */
  public List<DistributeHisPostClient.PostKeyToBrokerRes> postKeysToBrokerInCombine(Query query, String fromHost, Map<String, Set<Object>> aggNamesAndKeys){
    /**
     * 判断当前各key是否已经存在于某台work进行聚合，已存在的将该key发送至已有work
     * 全新key则优先判断本机是否正在聚合key，如果没有聚合key，则优先放入本机
     * 之后再将全新key分配到“当前所含key数最少的work主机”
     *
     * 最后更新记录每台主机当前正在聚合的agg+key个数
     */
    DistributeQueryPlan distributeQueryPlan = distributeQueryPlanMap.get(query.getId());

    List<DistributeHisPostClient.PostKeyToBrokerRes> postKeyToBrokerRes  = new ArrayList<>();
    for (Map.Entry<String, Set<Object>> aggNamesAndKeysEntry : aggNamesAndKeys.entrySet()) {
      String aggName = aggNamesAndKeysEntry.getKey();
      for (Object key : aggNamesAndKeysEntry.getValue()) {
        // 判断是否存在work正在聚合该key
        for (Map.Entry<String, Map<String, Set<Object>>> stringMapEntry : distributeQueryPlan.hostCombineStat.entrySet()) {
          Set<Object> objects = stringMapEntry.getValue().get(aggName);
        }
      }
    }

    return null;
  }

  // 在FinalMerge阶段，接收aggName，计算该聚合结果fv交由那台主机处理

  // 收到最终结果（收到后终止查询）

  // 接收通知，某台主机的agg对应的key combine合并完毕一次


  // 接收通知，某台主机的agg finalMerge合并完毕一次

  class DistributeQueryPlan{
    // 参与此次聚合的所有主机ip
    private List<String> workHosts;

    // key：his节点主机ip，value：需要返回的响应数量
    // 查询初始，每台his节点需要返回的segment分片查询数量（理论上来说，每个分片查询完毕都需要返回一个响应）
    private Map<String, AtomicInteger> hisHostsNeedRespCount;

    // key：主机ip，value：该主机还需要处理的聚合数
    private Map<String, AtomicInteger> hostNeedMergeCount = new ConcurrentHashMap<>();

    // key：主机ip，value：{key：aggName，value：key}
    // combine阶段，每台work主机上正在聚合的agg+key情况

    private Map<String, Map<String, Set<Object>>> hostCombineStat = new ConcurrentHashMap<>();

    // key：主机ip，value：aggName
    // final merge阶段，每台主机上正在聚合的agg情况
    private Map<String, Set<String>> hostFinalMergeStat = new ConcurrentHashMap<>();

    public DistributeQueryPlan(
            List<String> workHosts,
            Map<String, AtomicInteger> hisHostsNeedRespCount
    ){
      this.workHosts = workHosts;
      this.hisHostsNeedRespCount = hisHostsNeedRespCount;

      workHosts.forEach(workHost->{
        hostNeedMergeCount.put(workHost, new AtomicInteger(-1));
      });


    }
  }
}
