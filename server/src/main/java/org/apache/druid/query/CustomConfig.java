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
}
