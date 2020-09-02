package com.wankun.hadoop.reporter.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-08-31.
 */
public class JsonUtil {

  public static JSONObject readJsonObject(String file) throws IOException {
    String fileContent = FileUtils.readFileToString(new File(file), "UTF-8");
    return JSON.parseObject(fileContent);
  }

}
