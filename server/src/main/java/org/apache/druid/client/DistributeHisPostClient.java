package org.apache.druid.client;

import org.apache.druid.query.Query;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * !ndm
 * @author Chen768959
 * @date 2021/11/26
 */
public class DistributeHisPostClient {
  public static List<PostKeyToBrokerRes> postKeysToBrokerInCombine(Query query, Map<String, Set<Object>> aggNamesAndKeys) {
    return null;
  }

  public static void postKVToHisInCombine(Query query, PostKeyToBrokerRes res, Object value) {

  }

  public static List<DistributeHisPostClient.PostFVToBrokerRes> postFVToBrokerInFinal(Query query, Set<String> aggNames) {
    return null;
  }

  public static void postFVToHisInFinalMerge(Query query, PostFVToBrokerRes postFVToBrokerRes, Map<String, Object> finalValueRes) {

  }

  // 主动发送最终结果给broker
  public static void postResultObjectToBroker(Query query, Object result) {

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
