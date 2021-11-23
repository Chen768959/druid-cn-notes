package org.apache.druid.java.util.common.guava;

import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BinaryOperator;

/**
 * @author Chen768959
 * @date 2021/10/18
 */
public class ThreadPoolMergeCombiningSequence<T> extends YieldingSequenceBase<T> {
  private static final Logger LOG = new Logger(YieldingSequenceBase.class);
  private final List<Sequence<T>> inputSequences;
  private final BinaryOperator<T> combineFn;
  private final Ordering<T> orderingFn;

  ExecutorService fixedThreadPool = Executors.newFixedThreadPool(50);

  public ThreadPoolMergeCombiningSequence(
          List<Sequence<T>> inputSequences,
          BinaryOperator<T> combineFn,
          Ordering<T> orderingFn
  ){
    this.inputSequences = inputSequences;
    this.combineFn = combineFn;
    this.orderingFn = orderingFn;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator) {
    LOG.info("!!!，cons toYielder start:"+inputSequences.size());
    if (inputSequences.isEmpty()) {
      return Sequences.<T>empty().toYielder(initValue, accumulator);
    }

    PriorityBlockingQueue<T> cursorsT = new PriorityBlockingQueue<>(inputSequences.size());

    AtomicInteger needMergeCount = new AtomicInteger(inputSequences.size()-1);

    for (Sequence<T> s : inputSequences) {
      ParallelMergeCombiningSequence.YielderBatchedResultsCursor<T> cursor =
              new ParallelMergeCombiningSequence.YielderBatchedResultsCursor<>(
                      new ParallelMergeCombiningSequence.SequenceBatcher<>(s, 4096), orderingFn
              );
      /**
       * 开启一条新线程，每条线程会等待
       */
      fixedThreadPool.submit(new Runnable() {
        @Override
        public void run() {
          cursor.initialize();
          int c=0;
          while (!cursor.isDone()){
            needMergeCount.getAndAdd(c);
            T t = cursor.get();
            if (t!= null && !cursor.resultBatch.isDrained()){
              cursorsT.offer(t);
            }
            cursor.advance();
            if (c==0){
              c=1;
            }
          }
          try {
            cursor.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }

    AtomicInteger mergedCount = new AtomicInteger(0);
    T trueRes = null;
    boolean start=false;
    long lastTrueRes = System.currentTimeMillis();

    LOG.info("!!!，needMergeCount："+needMergeCount.get()+"...cursorsT.size:"+cursorsT.size());
    while (mergedCount.get() < needMergeCount.get() || cursorsT.size()>0){
      // cursorsT中装了his节点的响应数据，以及两两的聚合信息
      T poll = cursorsT.poll();

      if (poll != null){
        if (trueRes==null){// 第一次进来
          trueRes = poll;

          start=true;
          lastTrueRes = System.currentTimeMillis();
        }else {
          start=false;
          T tmp = trueRes;
          trueRes = null;

          fixedThreadPool.submit(new Runnable() {
            @Override
            public void run() {
              // 聚合之前的结果集
              /**{@link org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest#createMergeFn(Query)}*/
              T currentCombinedValue = combineFn.apply(tmp, poll);
              cursorsT.offer(currentCombinedValue);

              mergedCount.getAndIncrement();

            }
          });
        }
      }else {
        long timeC = System.currentTimeMillis() - lastTrueRes;
        if (timeC > 200000){
          fixedThreadPool.shutdownNow();
          break;
        }
        if (timeC > 20000){
          if (start && trueRes!=null){
            break;
          }else {
            LOG.info("!!!!，"+Thread.currentThread().getId()+"mergedCount："+mergedCount.get()+"needMergeCount："+needMergeCount.get()+"...cursorsT.size:"+cursorsT.size());
          }
        }
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          LOG.info("!!!!，"+e.getMessage());
        }
      }
    }

    // todo del2
//    PriorityBlockingQueue<ParallelMergeCombiningSequence.BatchedResultsCursor<T>> pQueue = new PriorityBlockingQueue<>();
//    PriorityBlockingQueue<T> cursorsT = new PriorityBlockingQueue<>();
//    for (Sequence<T> s : inputSequences) {
//
//      ParallelMergeCombiningSequence.SequenceBatcher<T> tSequenceBatcher = new ParallelMergeCombiningSequence.SequenceBatcher<>(s, 4096);
//
//      ParallelMergeCombiningSequence.YielderBatchedResultsCursor<T> tYielderBatchedResultsCursor = new ParallelMergeCombiningSequence.YielderBatchedResultsCursor<>(tSequenceBatcher, orderingFn);
//
//      tYielderBatchedResultsCursor.initialize();
//      if (!tYielderBatchedResultsCursor.isDone()) {
//        pQueue.offer(tYielderBatchedResultsCursor);
//      } else {
//        try {
//          tYielderBatchedResultsCursor.close();
//        } catch (IOException e) {
//          LOG.error(e.getMessage()+",0");
//        }
//      }
//    }
//
//    // 存储cursor中的结果
//    // 但是cursor结果并不能一次性get完
//    while (! pQueue.isEmpty()){
//      /**
//       * 每次取出一个cursor
//       * 判断cursor内数据是否已经取完了
//       * 取完则关闭该cursor，并取下一个cursor。
//       *
//       * 如果cursor内数据未取完，则开启新线程，然后从cursor中取一部分，然后将取出的数据放入“结果队列中”
//       */
//      ParallelMergeCombiningSequence.BatchedResultsCursor<T> cursor = pQueue.poll();
//      // push the queue along
//      if (!cursor.isDone()) {
//        fixedThreadPool.submit(new Runnable() {
//          @Override
//          public void run() {
//            T nextValueToAccumulate = cursor.get();
//            cursorsT.offer(nextValueToAccumulate);
//            cursor.advance();
//            if (!cursor.isDone()) {
//              pQueue.offer(cursor);
//            } else {
//              try {
//                cursor.close();
//              } catch (IOException e) {
//                LOG.error(e.getMessage()+",1");
//              }
//            }
//          }
//        });
//      } else {
//        try {
//          cursor.close();
//        } catch (IOException e) {
//          LOG.error(e.getMessage()+",2");
//        }
//      }
//    }
//
//    /**
//     * 每次从结果队列中取出一个结果，
//     * 与开启新线程，与上一个结果合并，然后将合并结果再放到结果队列中
//     */
//    T res = null;
//    while (! pQueue.isEmpty() || ! cursorsT.isEmpty()){
//      // cursorsT中装了his节点的响应数据，以及两两的聚合信息
//      T poll = cursorsT.poll();
//
//      if (poll != null){
//        if (res ==null){// 第一次进来
//          res = poll;
//        }else {
//          T tmp = res;
//          res = null;
//
//          fixedThreadPool.submit(new Runnable() {
//            @Override
//            public void run() {
//              // 聚合之前的结果集
//              T currentCombinedValue = combineFn.apply(tmp, poll);
//
//              cursorsT.offer(currentCombinedValue);
//            }
//          });
//        }
//      }else {
//        try {
//          Thread.sleep(500);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }
//    }

    // todo del
//    List<ParallelMergeCombiningSequence.BatchedResultsCursor<T>> partitionCursors = new ArrayList<>(inputSequences.size());
//    for (Sequence<T> s : inputSequences) {
//      partitionCursors.add(new ParallelMergeCombiningSequence.YielderBatchedResultsCursor<>(new ParallelMergeCombiningSequence.SequenceBatcher<>(s, 4096), orderingFn));
//    }
//
//    PriorityBlockingQueue<T> cursorsT = new PriorityBlockingQueue<>(inputSequences.size());
//
//    for (ParallelMergeCombiningSequence.BatchedResultsCursor<T> cursor : partitionCursors) {
//      fixedThreadPool.submit(new Runnable() {
//        @Override
//        public void run() {
//          cursor.initialize();// 一个cursor只能执行一次，初始化，且初始化过程就已经开始查询信息了，所以此方法必须并发
//          if (!cursor.isDone()) {
//            cursorsT.offer(cursor.get());
//          } else {
//            try {
//              cursor.close();
//            } catch (IOException e) {
//              e.printStackTrace();
//            }
//          }
//        }
//      });
//    }
//
//    // 计算需要聚合的次数
//    final int needMergeCount = partitionCursors.size()-1;
//    AtomicInteger mergedCount = new AtomicInteger(0);
//
//    T res = null;
//    while (mergedCount.get() < needMergeCount || cursorsT.size()>0){
//      LOG.info("!!!：his节点合并runner，whhh-mc000："+mergedCount.get()+"...whhh-nmc："+needMergeCount+"...whhh-sz:"+cursorsT.size());
//
//      // cursorsT中装了his节点的响应数据，以及两两的聚合信息
//      T poll = cursorsT.poll();
//
//      if (poll != null){
//        if (res ==null){// 第一次进来
//          res = poll;
//        }else {
//          T tmp = res;
//          res = null;
//
//          fixedThreadPool.submit(new Runnable() {
//            @Override
//            public void run() {
//              // 聚合之前的结果集
//              T currentCombinedValue = combineFn.apply(tmp, poll);
//
//              cursorsT.offer(currentCombinedValue);
//
//              mergedCount.getAndIncrement();
//            }
//          });
//        }
//      }else {
//        try {
//          Thread.sleep(500);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }
//    }
    LOG.info("!!!，end，mergedCount.get()："+mergedCount.get()+"...needMergeCount："+needMergeCount.get()+"...cursorsT.size:"+cursorsT.size());
    // yuan
    final BlockingQueue<ParallelMergeCombiningSequence.ResultBatch<T>> outputQueue = new ArrayBlockingQueue<>(inputSequences.size());
    ParallelMergeCombiningSequence.ResultBatch<T> outputBatch = new ParallelMergeCombiningSequence.ResultBatch<>(inputSequences.size());
    outputBatch.add(trueRes);
    outputQueue.offer(outputBatch);
    // ... and the terminal value to indicate the blocking queue holding the values is complete
    outputQueue.offer(ParallelMergeCombiningSequence.ResultBatch.TERMINAL);

    BaseSequence<T, Iterator<T>> finalOutSequence = new BaseSequence<>(
            new BaseSequence.IteratorMaker<T, Iterator<T>>() {
              @Override
              public Iterator<T> make() {
                return new Iterator<T>() {
                  private ParallelMergeCombiningSequence.ResultBatch<T> currentBatch;

                  @Override
                  public boolean hasNext() {

                    if (currentBatch != null && !currentBatch.isTerminalResult() && !currentBatch.isDrained()) {
                      return true;
                    }
                    try {
                      if (currentBatch == null || currentBatch.isDrained()) {
                        LOG.info("!!!,outputQueue2 Key:"+outputQueue.size());
                        currentBatch = outputQueue.take();
                      }

                      if (currentBatch.isTerminalResult()) {
                        return false;
                      }
                      return true;
                    } catch (InterruptedException e) {
                      throw new RE(e);
                    }
                  }

                  @Override
                  public T next() {
                    if (currentBatch == null || currentBatch.isDrained() || currentBatch.isTerminalResult()) {
                      throw new NoSuchElementException();
                    }
                    return currentBatch.next();
                  }
                };
              }

              @Override
              public void cleanup(Iterator<T> iterFromMake) {
              }
            }
    );

    Yielder<OutType> outTypeYielder = finalOutSequence.toYielder(initValue, accumulator);

    return outTypeYielder;
  }
}
