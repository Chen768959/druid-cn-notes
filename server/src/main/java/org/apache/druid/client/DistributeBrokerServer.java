package org.apache.druid.client;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.SegmentDescriptor;

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
    // 通过每台主机需要聚合的参数来判断combine阶段是否完毕，且本机完成后需要通知该机器combine完毕进入下一阶段
    combineContinue :while (true){
      for (AtomicInteger value : distributeQueryPlan.hostNeedMergeCount.values()) {
        if (value.get()!=0){
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

    // 此时所有主机


    // 判断
    return null;
  }
  // 开始查询，掌握所有涉及的his主机，以及所有参与聚合的主机


  // 在combine阶段，接收agg+key，计算该value交由那台主机处理

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
    private Map<String, AtomicInteger> hostNeedMergeCount = new HashMap<>();

    // key：主机ip，value：{key：aggName，value：key}
    // combine阶段，每台主机上正在聚合的agg+key情况
    private Map<String, Map<String, Object>> hostCombineStat = new HashMap<>();

    // key：主机ip，value：aggName
    // final merge阶段，每台主机上正在聚合的agg情况
    private Map<String, Set<String>> hostFinalMergeStat = new HashMap<>();

    public DistributeQueryPlan(
            List<String> workHosts,
            Map<String, AtomicInteger> hisHostsNeedRespCount
    ){
      this.workHosts = workHosts;
      this.hisHostsNeedRespCount = hisHostsNeedRespCount;
    }
  }
}
