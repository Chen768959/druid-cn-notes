package org.apache.druid.client;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.CustomConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.timeseries.TimeseriesResultValue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
   * v：该查询的combine阶段锁
   */
  private final Map<String, Object> allQueryCombineLock = new ConcurrentHashMap<>();

  /**
   * k：queryId
   * v：该查询的finalMerge阶段锁
   */
  private final Map<String, Object> allQueryFinalMergeLock = new ConcurrentHashMap<>();
  /**
   * k：queryId
   * v：true表示final阶段结束
   */
  private final Map<String, Boolean> allQueryFinalMergeIsEnd = new ConcurrentHashMap<>();

  /**
   * k：queryId
   * v：所有该查询的待聚合kv {k：agg结果的某个key   v：{p1：该key的锁   p2：该key的对应value结果}}
   */
  private final Map<String, Map<Object, Pair<Object, Object>>> allWaitCombineKV = new ConcurrentHashMap<>();

  /**
   * k：queryId
   * v：该查询在当前主机的所有等待final merge的fv结果队列
   */
  private final Map<String, Queue<Object>> allWaitFinalMergeFVRes = new ConcurrentHashMap<>();

  /**
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
   * 后续操作：
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
  public void startMerge(Query query, Sequence sequence){
    List<Result<TimeseriesResultValue>> aggKVList = (List<Result<TimeseriesResultValue>>) sequence.toList();

    distributeExecutor.submit(new Runnable() {
      @Override
      public void run() {
        // 1、准备一个线程池，用于本机相同k的combine
        ExecutorService localCombineExecutor = Executors.newFixedThreadPool(CustomConfig.getHisLocalCombineExecutorCount());

        // 本机相同key进行combine，每台his得到一组k-v
        Map<Object, Object> kvRes = localCombineBySameKey(localCombineExecutor, aggKVList);
        localCombineExecutor.shutdownNow();

        // 2、将所有本机k全部发送给broker，由broker判断是保留本机还是发送给其他his
        //准备此次查询的“等待combine集合”
        allWaitCombineKV.put(query.getId(), new ConcurrentHashMap<>());
        // 准备本次查询的combine锁
        Object combineLock = new Object();
        try {
          combineLock.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        allQueryCombineLock.put(query.getId(),combineLock);

        List<DistributeHisPostClient.PostKeyToBrokerRes> keysToBrokerRes = DistributeHisPostClient.postKeysToBrokerInCombine(query, kvRes.keySet());
        keysToBrokerRes.forEach(res->{
          Object value = kvRes.get(res.getKey());
          if (res.isLocal()){// 保留本地
            saveValueAndCombine(query, res.getKey(), value);
          }else {// 发送其他主机
            DistributeHisPostClient.postKVToHisInCombine(query, res, value);
          }
        });

        // 3、阻塞，直到收到broker的combine结束请求（期间一直在接收其他主机的kv结果并合并）
        synchronized (combineLock){
          allQueryCombineLock.remove(query.getId());
        }

        // 4、本机自行final待办map中的所有kv，得到final后的kv
        if (!allWaitCombineKV.get(query.getId()).isEmpty()){
          // 准备本次final使用的线程池
          ExecutorService localFinalExecutor = Executors.newFixedThreadPool(CustomConfig.getHisLocalFinalExecutorCount());
          Map<Object, Object> fkvRes = localFinalBySameKey(localFinalExecutor, allWaitCombineKV.get(query.getId()));

          // 5、本机自行final merge所有fkv
          // 准备本次final merge使用的线程池
          ExecutorService localFinalMergeExecutor = Executors.newFixedThreadPool(CustomConfig.getHisLocalFinalMergeExecutorCount());
          Object finalValueRes = localFinalMergeBySameKey(localFinalMergeExecutor, fkvRes);
          // 准备本次查询的final merge锁
          Object finalMergeLock = new Object();
          try {
            finalMergeLock.wait();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          allQueryFinalMergeLock.put(query.getId(),finalMergeLock);

          // 6、询问broker，由broker判断finalValueRes是保留本地进入待办队列，还是发送给其他his
          DistributeHisPostClient.PostFVToBrokerRes postFVToBrokerRes = DistributeHisPostClient.postFVToBrokerInFinal(query);
          if (postFVToBrokerRes.isLocal()){ // 保留本地
            saveFVAndFinalMerge(query, finalValueRes);
          }else { // 发送给其他his
            DistributeHisPostClient.postFVToHisInFinalMerge(query, postFVToBrokerRes, finalValueRes);
          }

          // 7、阻塞，直到收到broker的finalMerge结束请求（期间一直在接收其他主机的fv结果并合并）
          synchronized (finalMergeLock){
            allQueryFinalMergeLock.remove(query.getId());
          }
        }

        // 此次查询聚合完毕
        allThreadIsEnd.put(query.getId(), true);

        /**
         * broker收到某台主机keys后，先存起来，并返回信息让其原地待命。
         * 等到收到第二台的keys后，比较二者相同key，相同key就让第二台his主动发送给1his，其余key让2his待命。
         *
         * 1his收到2his发来的key后，主动接触阻塞，进行合并，
         * 合并完后发送消息给broker，告知其合并完毕，并由broker响应是等待，还是将新结果发于别人。
         * 等待则继续阻塞，发于别人则直接发送走。
         *
         * broker判断所有combine阶段完毕后，主动发送给所有his节点，接触他们的阻塞。
         * 每次his节点解除阻塞，先判断是否存在“待合并的k-v”，
         * 存在则合并后与broker交互，
         * 不存在则表示combine阶段完毕，此时是由broker唤醒的，则每个k-v线程进行下个阶段
         *
         * 每个k-v线程先进行自行的final，得到新k-v
         *
         * 每台his主机再进行自行的final v合并，每台主机都只剩一个v了
         *
         * 每台his请求broker，broker响应其是等待还是发送，
         * 每次final merge合并完后也发送broker告知，
         * broker判断所有final merge合并完后，发送通知唤醒所有his节点，再每台被唤醒的his都将自身剩余的v返回给broker
         * （但是正常情况，此时应该只有一台his节点有v了。这个v就是最终结果）
         *
         * broker拿到最终结果
         */
      }

      private Object localFinalMergeBySameKey(ExecutorService localFinalMergeExecutor, Map<Object, Object> fkvRes) {
        return null;
      }

      private Object finalMergeFV(Object fv1, Object fv2) {
        return null;
      }

      private Map<Object, Object> localFinalBySameKey(ExecutorService localFinalExecutor, Map<Object, Pair<Object, Object>> objectPairMap) {
        return null;
      }

      private Map<Object, Object> localCombineBySameKey(ExecutorService localCombineExecutor, List<Result<TimeseriesResultValue>> aggKVList) {
        return null;
      }
    });

  }

  /**
   * 阻塞{判断waitCombineMap中是否存在相同key的数据，如果存在，则获取key锁继续后续逻辑。如果不存在则创建锁后将数据存入waitCombineMap结束此次逻辑}
   * key锁阻塞{另起线程将此次value与wait value合并，并将合并后结果存入waitCombineMap}
   *
   * 合并成功后告知broker
   * @param query
   * @param key
   * @param value
   * @author Chen768959
   * @date 2021/11/26 下午 8:07
   * @return void
   */
  public void saveValueAndCombine(Query query, Object key, Object value){

  }

  /**
   * 阻塞{判断queue中是否存在对象，存在的话则取出，不存在则存入}
   * 另起线程合并新旧fv，再递归放入queue
   *
   * 合并成功后告知broker
   * @param query
   * @param fv
   * @author Chen768959
   * @date 2021/11/26 下午 9:00
   * @return void
   */
  public void saveFVAndFinalMerge(Query query, Object fv){
    allWaitFinalMergeFVRes.get(query.getId()).add(fv);


  }

  public void endCombine(Query query){
    Object combineLock = allQueryCombineLock.get(query.getId());
    if (combineLock!=null){
      combineLock.notify();
      allQueryCombineLock.remove(query.getId());
    }
  }

  public void endFinalMerge(Query query){
    Boolean finalIsEnd = allQueryFinalMergeIsEnd.get(query.getId());
    if (finalIsEnd!=null){
      finalIsEnd = true;
      allQueryFinalMergeIsEnd.remove(query.getId());
    }
  }

  public Object getFinalMergeResult(Query query){
    while (true){
      Boolean isEnd = Optional.ofNullable(allThreadIsEnd.get(query.getId())).orElse(false);
      if (isEnd){
        break;
      }
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    Object res = null;
    Queue<Object> waitFinalMergeFVRes = allWaitFinalMergeFVRes.get(query.getId());
    if (waitFinalMergeFVRes != null){
      if (!waitFinalMergeFVRes.isEmpty()){
        res = waitFinalMergeFVRes.poll();
      }
      allWaitFinalMergeFVRes.remove(query.getId());
    }

    return res;
  }

  public static DistributeMergeManager getInstance() {
    return distributeMergeManager;
  }
}
