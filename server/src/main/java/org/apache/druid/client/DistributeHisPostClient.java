package org.apache.druid.client;

import org.apache.druid.query.Query;
import org.apache.druid.query.timeseries.TimeseriesQuery;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * !ndm
 *
 * @author Chen768959
 * @date 2021/11/26
 */
public class DistributeHisPostClient {
  // 在combine阶段，询问broker，指定agg+key的value结果交由那台主机处理
  public static List<PostKeyToBrokerRes> postKeysToBrokerInCombine(Query query, Map<String, Set<Object>> aggNamesAndKeys) {
    return null;
  }

  // 在combine阶段，将agg结果发送至其他his
  public static void postKVToHisInCombine(Query query, PostKeyToBrokerRes res, Object value) {

  }

  // 在FinalMerge阶段，询问broker，指定agg的fv结果交由那台主机处理
  public static List<DistributeHisPostClient.PostFVToBrokerRes> postFVToBrokerInFinalMerge(Query query, Collection<String> aggNames) {
    return null;
  }

  // 在FinalMerge阶段，将fv结果发送至其他his
  public static void postFVToHisInFinalMerge(Query query, PostFVToBrokerRes postFVToBrokerRes, Map<String, Object> finalValueRes) {

  }

  // 主动发送最终结果给broker
  public static void postResultObjectToBroker(Query query, Object result) {

  }

  // 通知broker，agg对应的key combine合并完毕一次
  public static void postCombineNotifyToBroker(TimeseriesQuery query, String aggName, Object key) {

  }

  // 通知broker，agg finalMerge合并完毕一次
  public static void postFinalMergeNotifyToBroker(TimeseriesQuery query, String aggName) {

  }

  public class PostFVToBrokerRes{
    private boolean local;

    private String aggName;

    private String toUrl;

    public String getAggName() {
      return aggName;
    }

    public void setAggName(String aggName) {
      this.aggName = aggName;
    }

    public boolean isLocal() {
      return local;
    }

    public String getToUrl() {
      return toUrl;
    }

    public void setLocal(boolean local) {
      this.local = local;
    }

    public void setToUrl(String toUrl) {
      this.toUrl = toUrl;
    }
  }

  public class PostKeyToBrokerRes{
    private String aggName;

    private Object key;

    private boolean local;

    private String toUrl;

    public Object getKey() {
      return key;
    }

    public boolean isLocal() {
      return local;
    }

    public String getToUrl() {
      return toUrl;
    }

    public void setKey(Object key) {
      this.key = key;
    }

    public void setLocal(boolean local) {
      this.local = local;
    }

    public void setToUrl(String toUrl) {
      this.toUrl = toUrl;
    }

    public String getAggName() {
      return aggName;
    }

    public void setAggName(String aggName) {
      this.aggName = aggName;
    }
  }
}
