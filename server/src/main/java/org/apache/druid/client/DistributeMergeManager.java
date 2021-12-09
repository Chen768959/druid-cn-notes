package org.apache.druid.client;

import org.apache.druid.distribute.DistributeAggregatorFactory;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.CustomConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.checkerframework.checker.units.qual.A;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * !ndm
 * his节点准备线程操作集群集合逻辑
 * @author Chen768959
 * @date 2021/11/24
 */
public class DistributeMergeManager {
  private static DistributeMergeManager distributeMergeManager = new DistributeMergeManager();

  private final ExecutorService distributeExecutor;

  /**
   * k：queryId
   * v：该查询的combine阶段是否完毕（false为未完成）
   */
  private final Map<String, Boolean> allQueryCombineLock = new ConcurrentHashMap<>();

  /**
   * 待执行combine的kv数据
   * {k：queryId
   *  v：{k：aggName
   *      v：{k：agg结果的某个key
   *          v：待combine的value队列
   *         }
   *     }
   * }
   */
  private final Map<String, Map<String, Map<Object, Queue<Object>>>> allWaitCombineKV = new ConcurrentHashMap<>();

  /**
   * k：queryId
   * v：该查询在当前主机的所有等待final merge的fv结果队列
   */
  private final Map<String, Map<String, Queue<Object>>> allWaitFinalMergeFV = new ConcurrentHashMap<>();

  /**
   * （此map也同样用于判断当前主机某个查询是否开始聚合逻辑，为null就表示还在agg阶段）
   * k：queryId
   * v：该查询线程是否完毕
   */
  private final Map<String, Boolean> allThreadIsEnd = new ConcurrentHashMap<>();

  private DistributeMergeManager(){
    distributeExecutor = Executors.newFixedThreadPool(CustomConfig.getHisDistributeExecutorCount());
  }

  /**
   * 另起线程开始聚合操作
   *
   * 后续聚合操作：
   * 1、本机相同key进行combine，每台his得到一组k-v
   * 2、his交互broker，整个集群combine各个k-v，每台his得到一组k-v
   * 3、his final本机各个k-v，每台his得到一组新k-v
   * 4、his交互broker，整个集群进行“c final res”，剩余一台his主机一个v
   *
   * @param query 此次查询请求对象
   * @param sequence 当前节点所有待查询分片的agg k-v list结果
   * @author Chen768959
   * @date 2021/11/24 下午 3:18
   * @return void
   */
  public void startMerge(TimeseriesQuery query, Sequence sequence){
    /**
     * 单个seg分片对应一个Result对象
     * 单个Result中又只有一个TimeseriesResultValue对象，
     * 单个TimeseriesResultValue中包含了一个map对象，key是聚合器名，value是该聚合器对应的聚合结果
     */
    List<Result<TimeseriesResultValue>> aggKVList = (List<Result<TimeseriesResultValue>>) sequence.toList();

    // 0、准备此次聚合所需参数
    // 获取分布式聚合器
    Map<String, DistributeAggregatorFactory> queryDistributeAggregatorFactorys = new HashMap<>();
    List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
    for (AggregatorFactory aggregatorFactory : aggregatorSpecs) {
      if (aggregatorFactory.getClass().isAssignableFrom(DistributeAggregatorFactory.class)){
        queryDistributeAggregatorFactorys.put(aggregatorFactory.getName(), (DistributeAggregatorFactory)aggregatorFactory);
      }
    }
    if (queryDistributeAggregatorFactorys==null || queryDistributeAggregatorFactorys.isEmpty()){// 主动发送空响应
      DistributeHisPostClient.postResultObjectToBroker(query, null);
      return;
    }

    // 准备此次查询的“combine结果集合”
    Map<String, Map<Object, Queue<Object>>> waitCombineKV = new ConcurrentHashMap<>();
    allWaitCombineKV.put(query.getId(), waitCombineKV);

    // 准备此次查询的“final merge结果集合”
    Map<String, Queue<Object>> waitFinalMergeKV = new ConcurrentHashMap<>();
    allWaitFinalMergeFV.put(query.getId(), waitFinalMergeKV);

    // 建立查询合并持续标识
    allThreadIsEnd.put(query.getId(),false);

    // 开始聚合
    distributeExecutor.submit(new Runnable() {
      @Override
      public void run() {
        // 1、准备一个线程池，用于本机相同k的combine
        ExecutorService localCombineExecutor = Executors.newFixedThreadPool(CustomConfig.getHisLocalCombineExecutorCount());

        // 本机相同key进行combine，每台his得到一组k-v（按聚合器名group）
        Map<String, Map<Object, Object>> kvRes = localCombineBySameKey(localCombineExecutor, aggKVList, queryDistributeAggregatorFactorys);

        // 2、将所有本机k全部发送给broker，由broker判断是保留本机还是发送给其他his
        // 准备本次查询的combine锁
        allQueryCombineLock.put(query.getId(),false);

        // 提取出每个聚合器名与其包含的key
        Map<String, Set<Object>> aggNamesAndKeys = new HashMap<>();
        kvRes.entrySet().forEach(entry->{
          aggNamesAndKeys.put(entry.getKey(),entry.getValue().keySet());
        });

        List<DistributeHisPostClient.PostKeyToBrokerRes> keysToBrokerRes = DistributeHisPostClient.postKeysToBrokerInCombine(query, aggNamesAndKeys);
        keysToBrokerRes.forEach(res->{
          Object value = kvRes.get(res.getAggName()).get(res.getKey());
          if (res.isLocal()){// 保留本地
            saveValueInCombine(query, res.getAggName(), res.getKey(), value);
          }else {// 发送其他主机
            DistributeHisPostClient.postKVToHisInCombine(query, res, value);
          }
        });

        // 3、阻塞，循环合并，直到收到broker的combine结束请求
        while (! allQueryCombineLock.get(query.getId())){
          waitCombineKV.forEach((aggName, kvMap)->{
            DistributeAggregatorFactory distributeAggregatorFactory = queryDistributeAggregatorFactorys.get(aggName);
            kvMap.forEach((key,valueQueue)->{
              if (valueQueue.size()>1){
                Object value1 = valueQueue.poll();
                Object value2 = valueQueue.poll();
                localCombineExecutor.submit(()->{
                  Object combineRes = distributeAggregatorFactory.combine(value1, value2);
                  valueQueue.add(combineRes);
                  DistributeHisPostClient.postCombineNotifyToBroker(query, aggName, key);
                });
              }
            });
          });
        }
        localCombineExecutor.shutdownNow();// 关闭combine线程池

        if (!allWaitCombineKV.get(query.getId()).isEmpty()){
          // 准备本次final使用的线程池
          ExecutorService localFinalExecutor = Executors.newFixedThreadPool(CustomConfig.getHisLocalFinalExecutorCount());
          // 4、本机自行final所有的combine结果，得到各个final后的kv
          Map<String, Map<Object, Object>> fkvRes = localFinalBySameKey(localFinalExecutor, allWaitCombineKV.get(query.getId()), queryDistributeAggregatorFactorys);// 获取当前节点所有等待final的kv结果
          localFinalExecutor.shutdownNow();

          // 准备本次final merge使用的线程池
          ExecutorService localFinalMergeExecutor = Executors.newFixedThreadPool(CustomConfig.getHisLocalFinalMergeExecutorCount());
          // 5、本机自行final merge所有fkv，每个agg得到一个value结果
          Map<String, Object> finalValueRes = localFinalMergeBySameKey(localFinalMergeExecutor, fkvRes, queryDistributeAggregatorFactorys);// k:aggName   v:该聚合器对应结果


          // 6、询问broker，由broker判断finalValueRes是保留本地进入待办队列，还是发送给其他his
          List<DistributeHisPostClient.PostFVToBrokerRes> postFVToBrokerResList = DistributeHisPostClient.postFVToBrokerInFinalMerge(query, finalValueRes.keySet());
          postFVToBrokerResList.forEach(postFVToBrokerRes->{
            if (postFVToBrokerRes.isLocal()){ // 保留本地
              saveFVInFinalMerge(query, postFVToBrokerRes.getAggName(), finalValueRes.get(postFVToBrokerRes.getAggName()));
            }else { // 发送给其他his
              DistributeHisPostClient.postFVToHisInFinalMerge(query, postFVToBrokerRes, finalValueRes);
            }
          });

          // 7、阻塞，循环final Merge，直到收到broker的完结请求
          while (!allThreadIsEnd.get(query.getId())){
            waitFinalMergeKV.forEach((aggName, valueQueue)->{
              DistributeAggregatorFactory distributeAggregatorFactory = queryDistributeAggregatorFactorys.get(aggName);
              if (valueQueue.size()>1){
                Object fmv1 = valueQueue.poll();
                Object fmv2 = valueQueue.poll();
                localFinalMergeExecutor.submit(()->{
                  Object fmvRes = distributeAggregatorFactory.finalizedMerge(fmv1, fmv2);

                  // 通知broker fm合并完毕
                  DistributeHisPostClient.postFinalMergeNotifyToBroker(query, aggName);
                  // 询问broker此结果去向
                  DistributeHisPostClient.PostFVToBrokerRes postFVToBrokerRes = DistributeHisPostClient.postFVToBrokerInFinalMerge(query, new ArrayList<String>() {{ add(aggName); }}).get(0);
                  if (postFVToBrokerRes.isLocal()){ // 保留本地
                    saveFVInFinalMerge(query, postFVToBrokerRes.getAggName(), fmvRes);
                  }else { // 发送给其他his
                    DistributeHisPostClient.postFVToHisInFinalMerge(query, postFVToBrokerRes, new HashMap<String, Object>(){{put(aggName, fmvRes);}});
                  }
                });
              }
            });
          }
          localFinalMergeExecutor.shutdownNow();
        }
      }

      /**
       * 本机执行相同key combine
       * @param localCombineExecutor
       * @param aggKVResultList
       * @param queryDistributeAggregatorFactorys
       * @author Chen768959
       * @date 2021/12/2 下午 3:35
       * @return java.util.Map<java.lang.String,java.util.Map<java.lang.Object,java.lang.Object>> 外围map key是aggName，内部map key是每个agg结果key及对应value值
       */
      private Map<String, Map<Object, Object>> localCombineBySameKey(ExecutorService localCombineExecutor, List<Result<TimeseriesResultValue>> aggKVResultList, Map<String, DistributeAggregatorFactory> queryDistributeAggregatorFactorys) {
        // k：aggName  v：待聚合kv对象
        Map<String, Map<Object, Pair<AtomicInteger, Queue<Object>>>> waitMergeAggQueue = new HashMap<>();

        // 先将所有待聚合对象装入队列，然后计算总聚合次数
        aggKVResultList.forEach(result->{
          Map<String, Object> aggNameAndKVMap = result.getValue().getBaseObject();// 单个分片结果：聚合器与对应kv结果
          aggNameAndKVMap.forEach((aggName, kvMap)->{// 遍历每一个aggName与对应kvMap
            Map<Object, Pair<AtomicInteger, Queue<Object>>> kvQueue = waitMergeAggQueue.get(aggName);
            if (kvQueue==null){
              kvQueue = new HashMap<>();
              waitMergeAggQueue.put(aggName, kvQueue);
            }
            for (Map.Entry<Object, Object> kvEntry : ((Map<Object, Object>) kvMap).entrySet()) {// 遍历每一个kv结果
              Pair<AtomicInteger, Queue<Object>> waitValueQueuePair = kvQueue.get(kvEntry.getKey());// 当前queue所需聚合次数 + 带聚合queue队列
              if (waitValueQueuePair==null){
                waitValueQueuePair = new Pair<>(new AtomicInteger(-1), new ConcurrentLinkedQueue<>());
              }
              waitValueQueuePair.lhs.getAndIncrement();
              waitValueQueuePair.rhs.add(kvEntry.getValue());
            }
          });
        });

        // 循环聚合，直至所有key聚合完毕
        try {
          new ForkJoinPool(waitMergeAggQueue.size()).submit(()->{
            waitMergeAggQueue.entrySet().stream().parallel().forEach((entry)->{
              String aggName = entry.getKey();
              Map<Object, Pair<AtomicInteger, Queue<Object>>> kvWaitQueue = entry.getValue();
              DistributeAggregatorFactory distributeAggregatorFactory = queryDistributeAggregatorFactorys.get(aggName);// 当前aggname对应聚合器
              kvWaitQueue.forEach((key, mergeCountAndWaitQueue)->{
                AtomicInteger needMergeCount = mergeCountAndWaitQueue.lhs;
                Queue<Object> waitMergeQueue = mergeCountAndWaitQueue.rhs;
                while (needMergeCount.get()>0){
                  Object value1 = null;
                  Object value2 = null;
                  synchronized (waitMergeQueue){
                    if (waitMergeQueue.size()>1){
                      value1 = waitMergeQueue.poll();
                      value2 = waitMergeQueue.poll();
                    }
                  }
                  if (value1!=null && value2!=null){
                    Object finalValue1 = value1;
                    Object finalValue2 = value2;
                    localCombineExecutor.submit(()->{// 聚合
                      Object combineRes = distributeAggregatorFactory.combine(finalValue1, finalValue2);
                      waitMergeQueue.add(combineRes);
                      needMergeCount.getAndDecrement();
                    });
                  }else {
                    try {
                      Thread.sleep(1000);
                    } catch (InterruptedException e) {
                      e.printStackTrace();
                    }
                  }
                }
              });
            });
          }).get();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }

        // 所有key都聚合完毕
        Map<String, Map<Object, Object>> result = new HashMap<>();
        waitMergeAggQueue.forEach((aggName, kvMap)->{
          Map<Object, Object> kvMapResult = new HashMap<>();
          kvMap.forEach((key, valuePair)->{
            kvMapResult.put(key, valuePair.rhs.poll());
          });
          result.put(aggName, kvMapResult);
        });

        return result;
      }

      /**
       * 当前每个key都combine结束，且对应一个value，此时执行每个kv的final逻辑
       * @param localFinalExecutor
       * @param objectPairMap
       * @author Chen768959
       * @date 2021/12/3 上午 10:53
       * @return java.util.Map<java.lang.String,java.util.Map<java.lang.Object,java.lang.Object>>
       */
      private Map<String, Map<Object, Object>> localFinalBySameKey(ExecutorService localFinalExecutor, Map<String, Map<Object, Queue<Object>>> objectPairMap, Map<String, DistributeAggregatorFactory> queryDistributeAggregatorFactorys) {
        Map<String, Map<Object, Object>> finalResMap = new HashMap<>();

        objectPairMap.forEach((aggName, kvRes)->{
          DistributeAggregatorFactory distributeAggregatorFactory = queryDistributeAggregatorFactorys.get(aggName);// 当前聚合器
          Map<Object, Object> kvFinalResMap= new ConcurrentHashMap<>();
          finalResMap.put(aggName, kvFinalResMap);
          kvRes.forEach((key, valueQueue)->{
            localFinalExecutor.submit(()->{
              Object finalRes = distributeAggregatorFactory.finalizeComputation(valueQueue.poll());
              kvFinalResMap.put(key, finalRes);
            });
          });
        });
        localFinalExecutor.shutdown();
        while (true){
          if (localFinalExecutor.isTerminated()){
            break;
          }
        }

        return finalResMap;
      }

      /**
       * 将多个final结果合并为一个，最终输出每个agg聚合器的final结果
       * @param localFinalMergeExecutor
       * @param fkvRes
       * @param queryDistributeAggregatorFactorys
       * @author Chen768959
       * @date 2021/12/3 下午 2:23
       * @return java.util.Map<java.lang.String,java.lang.Object>
       */
      private Map<String, Object> localFinalMergeBySameKey(ExecutorService localFinalMergeExecutor, Map<String, Map<Object, Object>> fkvRes, Map<String, DistributeAggregatorFactory> queryDistributeAggregatorFactorys) {
        Map<String, Pair<AtomicInteger, Queue<Object>>> finalMergeWaitQueueMap = new HashMap<>();

        fkvRes.forEach((aggName, fkvMap)->{
          AtomicInteger needMergeCount = new AtomicInteger(-1);
          Queue<Object> finalMergeWaitQueue = new ConcurrentLinkedQueue<>();
          finalMergeWaitQueueMap.put(aggName, new Pair<>(needMergeCount, finalMergeWaitQueue));
          fkvMap.values().forEach(fv->{
            finalMergeWaitQueue.add(fv);
            needMergeCount.getAndIncrement();
          });
        });

        try {
          new ForkJoinPool(fkvRes.size()).submit(()->{
            finalMergeWaitQueueMap.entrySet().stream().parallel().forEach(entry->{
              String aggName = entry.getKey();
              DistributeAggregatorFactory distributeAggregatorFactory = queryDistributeAggregatorFactorys.get(aggName);
              Pair<AtomicInteger, Queue<Object>> fvQueuePair = entry.getValue();
              if (fvQueuePair.lhs.get() > 0){
                Object fv1 = null;
                Object fv2 = null;
                synchronized (fvQueuePair.rhs){
                  if (fvQueuePair.rhs.size()>1){
                    fv1 = fvQueuePair.rhs.poll();
                    fv2 = fvQueuePair.rhs.poll();
                  }
                }
                if (fv1!=null && fv2!=null){
                  Object finalFv = fv1;
                  Object finalFv1 = fv2;
                  localFinalMergeExecutor.submit(()->{
                    Object fvRes = distributeAggregatorFactory.finalizedMerge(finalFv, finalFv1);
                    fvQueuePair.rhs.add(fvRes);
                    fvQueuePair.lhs.getAndDecrement();
                  });
                }
              }
            });
          }).get();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }

        Map<String, Object> fvMergeResMap = new HashMap<>();
        finalMergeWaitQueueMap.forEach((aggName,fvMergeResQueue)->{
          fvMergeResMap.put(aggName, fvMergeResQueue.rhs.poll());
        });

        return fvMergeResMap;
      }
    });
  }

  /**
   * 在combine阶段，存入待combine value
   * @param query
   * @param key
   * @param value
   * @author Chen768959
   * @date 2021/11/26 下午 8:07
   * @return void
   */
  public void saveValueInCombine(Query query,String aggName, Object key, Object value){
    Map<String, Map<Object, Queue<Object>>> waitCombineAggNameKVQueueMap = allWaitCombineKV.get(query);
    synchronized (waitCombineAggNameKVQueueMap){
      Map<Object, Queue<Object>> waitCombineKVQueueMap = waitCombineAggNameKVQueueMap.get(aggName);
      if (waitCombineKVQueueMap==null){
        waitCombineKVQueueMap = new ConcurrentHashMap<>();
        waitCombineAggNameKVQueueMap.put(aggName, waitCombineKVQueueMap);
      }

      Queue<Object> waitCombineValueQueue = waitCombineKVQueueMap.get(key);
      if (waitCombineValueQueue==null){
        waitCombineValueQueue = new ConcurrentLinkedQueue<>();
        waitCombineKVQueueMap.put(key, waitCombineValueQueue);
      }

      waitCombineValueQueue.add(value);
    }
  }

  /**
   * 在finalMerge阶段存入finalValue
   * @param query
   * @param fv
   * @author Chen768959
   * @date 2021/11/26 下午 9:00
   * @return void
   */
  public void saveFVInFinalMerge(Query query, String aggName, Object fv){
    Map<String, Queue<Object>> waitFinalMergeFV = allWaitFinalMergeFV.get(query.getId());
    synchronized (waitFinalMergeFV){
      Queue<Object> waitFinalMergeFVAggQueue = waitFinalMergeFV.get(aggName);
      if (waitFinalMergeFVAggQueue == null){
        waitFinalMergeFVAggQueue = new ConcurrentLinkedQueue<>();
        waitFinalMergeFV.put(aggName, waitFinalMergeFVAggQueue);
      }

      waitFinalMergeFVAggQueue.add(fv);
    }
  }

  public void endCombine(Query query){
    allQueryCombineLock.put(query.getId(),true);
  }

  /**
   * 获取某查询的聚合结果
   * （通常由broker请求调用）
   * @param query
   * @author Chen768959
   * @date 2021/12/3 下午 6:25
   * @return java.util.Map<java.lang.String,java.lang.Object>
   */
  public Map<String, Object> getFinalMergeResult(Query query){
    Map<String, Queue<Object>> waitFinalMergeFV = allWaitFinalMergeFV.get(query.getId());
    Map<String, Object> resAggMap = null;
    if (!waitFinalMergeFV.isEmpty()){
      waitFinalMergeFV.forEach((aggName,fvQueue)->{
        resAggMap.put(aggName, Optional.ofNullable(fvQueue).orElse(new PriorityQueue<>()).poll());
      });
    }

    return resAggMap;
  }

  /**
   * 判断某个查询的聚合阶段是否已经开始
   * @param query
   * @author Chen768959
   * @date 2021/12/3 下午 5:46
   * @return boolean
   */
  public boolean queryMergeIsStart(Query query){
    return (allThreadIsEnd.get(query.getId())!=null);
  }

  /**
   * 关闭指定query的聚合逻辑
   * （获取聚合结果需要在此方法前被调用）
   * @param query
   * @author Chen768959
   * @date 2021/12/3 下午 6:24
   * @return void
   */
  public void endQuery(Query query){
    allQueryCombineLock.remove(query.getId());
    allWaitCombineKV.remove(query.getId());
    allWaitFinalMergeFV.remove(query.getId());
    allThreadIsEnd.remove(query.getId());
  }

  public static DistributeMergeManager getInstance() {
    return distributeMergeManager;
  }
}
