package org.apache.druid.query.aggregation;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/11/24
 */
public interface DistributeAggregator extends Aggregator {
  @Nullable
  Map<Object,Object> getKV();
}
