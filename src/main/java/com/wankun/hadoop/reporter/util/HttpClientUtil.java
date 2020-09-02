package com.wankun.hadoop.reporter.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;


/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-08-31.
 */
public class HttpClientUtil {
  protected static Log logger = LogFactory.getLog(HttpClientUtil.class);

  private static PoolingHttpClientConnectionManager cm;
  private static String EMPTY_STR = "";
  private static String UTF_8 = "UTF-8";

  private static void init() {
    if (cm == null) {
      cm = new PoolingHttpClientConnectionManager();
      cm.setMaxTotal(100);// 整个连接池最大连接数
      cm.setDefaultMaxPerRoute(5);// 每路由最大连接数，默认值是2
    }
  }

  /**
   * 通过连接池获取HttpClient
   *
   * @return
   */
  public static CloseableHttpClient getHttpClient() {
    init();
    return HttpClients.custom().setConnectionManager(cm).build();
  }

  public static String httpGetRequest(String url) throws IOException {
    HttpGet httpGet = new HttpGet(url);
    return getResult(httpGet);
  }

  public static String httpGetRequest(String url, Map<String, Object> params) throws URISyntaxException, IOException {
    URIBuilder ub = new URIBuilder();
    ub.setPath(url);

    ArrayList<NameValuePair> pairs = covertParams2NVPS(params);
    ub.setParameters(pairs);

    HttpGet httpGet = new HttpGet(ub.build());

    return getResult(httpGet);
  }

  public static String httpGetRequest(String url, Map<String, Object> headers, Map<String, Object> params)
      throws URISyntaxException, IOException {
    URIBuilder ub = new URIBuilder();
    ub.setPath(url);

    ArrayList<NameValuePair> pairs = covertParams2NVPS(params);
    ub.setParameters(pairs);

    HttpGet httpGet = new HttpGet(ub.build());
    for (Map.Entry<String, Object> param : headers.entrySet()) {
      if (param.getValue() != null) {
        httpGet.addHeader(param.getKey(), String.valueOf(param.getValue()));
      }
    }
    return getResult(httpGet);
  }

  public static String httpPostRequest(String url) throws IOException {
    HttpPost httpPost = new HttpPost(url);
    return getResult(httpPost);
  }

  public static String httpPostRequest(String url, Map<String, Object> params) throws IOException {
    HttpPost httpPost = new HttpPost(url);
    ArrayList<NameValuePair> pairs = covertParams2NVPS(params);
    httpPost.setEntity(new UrlEncodedFormEntity(pairs, UTF_8));
    return getResult(httpPost);
  }

  public static String httpPostRequest(String url, Map<String, Object> headers, Map<String, Object> params)
      throws IOException {
    HttpPost httpPost = new HttpPost(url);

    for (Map.Entry<String, Object> param : headers.entrySet()) {
      if (param.getValue() != null) {
        httpPost.addHeader(param.getKey(), String.valueOf(param.getValue()));
      }
    }

    ArrayList<NameValuePair> pairs = covertParams2NVPS(params);
    httpPost.setEntity(new UrlEncodedFormEntity(pairs, UTF_8));

    return getResult(httpPost);
  }

  public static String httpPostRequest(String url, Map<String, Object> headers, String strBody)
      throws Exception {
    HttpPost httpPost = new HttpPost(url);

    for (Map.Entry<String, Object> param : headers.entrySet()) {
      if (param.getValue() != null) {
        httpPost.addHeader(param.getKey(), String.valueOf(param.getValue()));
      }
    }
    httpPost.setEntity(new StringEntity(strBody, UTF_8));
    return getResult(httpPost);
  }

  private static ArrayList<NameValuePair> covertParams2NVPS(Map<String, Object> params) {
    ArrayList<NameValuePair> pairs = new ArrayList<NameValuePair>();
    for (Map.Entry<String, Object> param : params.entrySet()) {
      if (param.getValue() == null) {
        pairs.add(new BasicNameValuePair(param.getKey(), ""));
      } else {
        pairs.add(new BasicNameValuePair(param.getKey(), String.valueOf(param.getValue())));
      }
    }

    return pairs;
  }

  /**
   * 处理Http请求
   *
   *  setConnectTimeout：设置连接超时时间，单位毫秒。
   *  setConnectionRequestTimeout：设置从connect Manager获取Connection 超时时间，单位毫秒。这个属性是新加的属性，因为目前版本是可以共享连接池的。
   *  setSocketTimeout：请求获取数据的超时时间，单位毫秒。 如果访问一个接口，多少时间内无法返回数据，就直接放弃此次调用。
   *
   * @param request
   * @return
   */
  private static String getResult(HttpRequestBase request) throws IOException {
    RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectTimeout(30000)
            .setConnectionRequestTimeout(5000)
            .setSocketTimeout(30000).build();
    request.setConfig(requestConfig);// 设置请求和传输超时时间

    CloseableHttpClient httpClient = getHttpClient();
    try {
      CloseableHttpResponse response = httpClient.execute(request);
      if (response.getStatusLine().getStatusCode() != 200) {
        logger.warn("请求失败! request = " + request);
        return EMPTY_STR;
      }
      // response.getStatusLine().getStatusCode();
      HttpEntity entity = response.getEntity();
      if (entity != null) {
        // long len = entity.getContentLength();// -1 表示长度未知
        String result = EntityUtils.toString(entity);
        response.close();
        return result;
      }
    } catch (ClientProtocolException e) {
      logger.error("[maperror] HttpClientUtil ClientProtocolException : " + e.getMessage());
      throw e;
    } catch (IOException e) {
      logger.error("[maperror] HttpClientUtil IOException : " + e.getMessage());
      throw e;
    }
    return EMPTY_STR;
  }
}
