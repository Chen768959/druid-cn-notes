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
  public static List<PostKeyToBrokerRes> postKeysToBrokerInCombine(Query query, Set<Object> keySet) {
    return null;
  }

  public static void postKVToHisInCombine(Query query, PostKeyToBrokerRes res, Object value) {

  }

  public static PostFVToBrokerRes postFVToBrokerInFinal(Query query) {
    return null;
  }

  public static void postFVToHisInFinalMerge(Query query, PostFVToBrokerRes postFVToBrokerRes, Object finalValueRes) {

  }

  public class PostFVToBrokerRes{
    private boolean local;

    private String toUrl;

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
  }
}
