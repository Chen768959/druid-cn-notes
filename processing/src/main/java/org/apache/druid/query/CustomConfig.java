package org.apache.druid.query;

import org.apache.druid.java.util.common.logger.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Chen768959
 * @date 2021/11/4
 */
public class CustomConfig {
  private static final Logger LOG = new Logger(CustomConfig.class);

  public static final String DISTRIBUTE_MERGE = "distributeMerge";

  public static boolean needQuickMerge(Query query){
    boolean needQuickMerge = false;

    if ("true".equals(query.getContextValue("quickMerge"))){
      needQuickMerge = true;
    }else if ((!"false".equals(query.getContextValue("quickMerge")))){
      FileInputStream fileInputStream = null;
      InputStreamReader inputStreamReader = null;
      BufferedReader bufferedReader = null;
      try {
        File file = new File("/data/druid-config/quickMerge.config");
        if (file.exists()){
          fileInputStream = new FileInputStream(file);
          inputStreamReader = new InputStreamReader(fileInputStream);
          bufferedReader = new BufferedReader(inputStreamReader);
          String dataSourceName = "";
          config: while ((dataSourceName = bufferedReader.readLine()) != null) {
            for (String tableName : query.getDataSource().getTableNames()) {
              LOG.info("!!!："+Thread.currentThread().getId()+"...query datasource："+tableName);
              if (dataSourceName.equals(tableName) || "all".equals(dataSourceName)){
                needQuickMerge = true;
                break config;
              }
            }
          }
        }
      }catch (Exception e){
        LOG.warn("quickMerge.config read error");
      }finally {
        try {
          if (fileInputStream!=null){
            fileInputStream.close();
          }
          if (inputStreamReader!=null){
            inputStreamReader.close();
          }
          if (bufferedReader!=null){
            bufferedReader.close();
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return needQuickMerge;
  }

  public static boolean needDistributeMerge(Query query){
    boolean needDistributeMerge = false;

    if ("true".equals(query.getContextValue(DISTRIBUTE_MERGE))){
      needDistributeMerge = true;
    }else if ((!"false".equals(query.getContextValue(DISTRIBUTE_MERGE)))){
      FileInputStream fileInputStream = null;
      InputStreamReader inputStreamReader = null;
      BufferedReader bufferedReader = null;
      try {
        File file = new File("/data/druid-config/distributeMerge.config");
        if (file.exists()){
          fileInputStream = new FileInputStream(file);
          inputStreamReader = new InputStreamReader(fileInputStream);
          bufferedReader = new BufferedReader(inputStreamReader);
          String dataSourceName = "";
          config: while ((dataSourceName = bufferedReader.readLine()) != null) {
            for (String tableName : query.getDataSource().getTableNames()) {
              if (dataSourceName.equals(tableName) || "all".equals(dataSourceName)){
                needDistributeMerge = true;
                break config;
              }
            }
          }
        }
      }catch (Exception e){
        LOG.warn("distributeMerge.config read error");
      }finally {
        try {
          if (fileInputStream!=null){
            fileInputStream.close();
          }
          if (inputStreamReader!=null){
            inputStreamReader.close();
          }
          if (bufferedReader!=null){
            bufferedReader.close();
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return needDistributeMerge;
  }

  // his节点并发查询segment的线程池
  public static int getHisSegExecutorCount(){
    int executorCount = 0;
    FileInputStream fileInputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null;
    try {
      File file = new File("/data/druid-config/hisSegExecutorCount.config");
      if (file.exists()){
        fileInputStream = new FileInputStream(file);
        inputStreamReader = new InputStreamReader(fileInputStream);
        bufferedReader = new BufferedReader(inputStreamReader);
        executorCount = Integer.parseInt(bufferedReader.readLine());
      }
    }catch (Exception e){
      LOG.warn("distributeMerge.config read error");
    }finally {
      try {
        if (fileInputStream!=null){
          fileInputStream.close();
        }
        if (inputStreamReader!=null){
          inputStreamReader.close();
        }
        if (bufferedReader!=null){
          bufferedReader.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return executorCount;
  }

  // his节点分布式聚合中，并发处理查询请求的线程池（关系到集群可并发处理请求的数量）
  public static int getHisDistributeExecutorCount(){
    int executorCount = 10;
    FileInputStream fileInputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null;
    try {
      File file = new File("/data/druid-config/hisDistributeExecutorCount.config");
      if (file.exists()){
        fileInputStream = new FileInputStream(file);
        inputStreamReader = new InputStreamReader(fileInputStream);
        bufferedReader = new BufferedReader(inputStreamReader);
        executorCount = Integer.parseInt(bufferedReader.readLine());
      }
    }catch (Exception e){
      LOG.warn("distributeMerge.config read error");
    }finally {
      try {
        if (fileInputStream!=null){
          fileInputStream.close();
        }
        if (inputStreamReader!=null){
          inputStreamReader.close();
        }
        if (bufferedReader!=null){
          bufferedReader.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return executorCount;
  }

  // his节点单台主机combine自身k-v的线程池（关系到agg完毕后，第一步本机combine的并发量）
  public static int getHisLocalCombineExecutorCount(){
    int executorCount = 10;
    FileInputStream fileInputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null;
    try {
      File file = new File("/data/druid-config/hisLocalCombineExecutorCount.config");
      if (file.exists()){
        fileInputStream = new FileInputStream(file);
        inputStreamReader = new InputStreamReader(fileInputStream);
        bufferedReader = new BufferedReader(inputStreamReader);
        executorCount = Integer.parseInt(bufferedReader.readLine());
      }
    }catch (Exception e){
      LOG.warn("distributeMerge.config read error");
    }finally {
      try {
        if (fileInputStream!=null){
          fileInputStream.close();
        }
        if (inputStreamReader!=null){
          inputStreamReader.close();
        }
        if (bufferedReader!=null){
          bufferedReader.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return executorCount;
  }


  public static int getHisLocalFinalExecutorCount() {
    return 0;
  }

  public static int getHisLocalFinalMergeExecutorCount() {
    return 0;
  }
}
