package org.apache.druid.distribute;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;

/**
 * @author Chen768959
 * @date 2021/11/24
 */
public abstract class DistributeAggregatorFactory extends AggregatorFactory {

  public abstract DistributeAggregator factorizeDistribute(ColumnSelectorFactory metricFactory);

  @Nullable
  public abstract Object finalizedMerge(@Nullable Object lfinalized, @Nullable Object rfinalized);

  public abstract byte[] serializerCombineRes(Object combineRes);

  public abstract Object deserializerCombineRes(byte[] combineRes);

  public abstract byte[] serializerFinalizeRes(Object finalizeRes);

  public abstract Object deserializerFinalizeRes(byte[] finalizeRes);
}