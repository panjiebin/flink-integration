package com.pan.flink.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author panjb
 */
public class HttpHelper {

    private final static Logger logger = LoggerFactory.getLogger(HttpHelper.class);

    private final static PoolingHttpClientConnectionManager CM;
    private final static ObjectMapper MAPPER = new ObjectMapper();

    static {
        // 创建http连接池，可以同时指定连接超时时间
        CM = new PoolingHttpClientConnectionManager(60000, TimeUnit.MILLISECONDS);
        // 最多同时连接20个请求
        CM.setMaxTotal(20);
        // 每个路由最大连接数，路由指IP+PORT或者域名
        CM.setDefaultMaxPerRoute(50);
    }

    private HttpHelper() {
    }

    public static CloseableHttpClient getHttpClient(){
        return getHttpClient(null, null);
    }

    public static CloseableHttpClient getHttpClient(String userName, String password) {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(1000)
                .setConnectionRequestTimeout(3000)
                .setSocketTimeout(10 * 1000)
                .build();
        HttpClientBuilder httpClientBuilder = HttpClients.custom();
        if (StringUtils.isNotBlank(userName)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
        return httpClientBuilder
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(CM)
                .build();
    }

    public static <T> T doGet(String url, TypeReference<T> type) {
        return doGet(url, null, type);
    }

    public static <T> T doGet(String url, Map<String, String> headers, TypeReference<T> type) {
        HttpGet httpGet = new HttpGet(url);
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpGet.addHeader(entry.getKey(), entry.getValue());
            }
        }
        CloseableHttpResponse httpResponse = null;
        try {
            CloseableHttpClient httpClient = getHttpClient();
            httpResponse = httpClient.execute(httpGet);
            return parseResponse(httpResponse, type);
        } catch (IOException e) {
            if (logger.isErrorEnabled()) {
                logger.error("http request error", e);
            }
        } finally {
            try {
                //执行httpResponse.close关闭对象会关闭连接池，
                //如果需要将连接释放到连接池，可以使用EntityUtils.consume()方法
                if (httpResponse != null) {
                    EntityUtils.consume(httpResponse.getEntity());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static <T> T doPost(String url, Map<String, String> headers, TypeReference<T> type, String payload) {
        HttpPost httpPost = new HttpPost(url);
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpPost.addHeader(entry.getKey(), entry.getValue());
            }
        }
        CloseableHttpResponse httpResponse = null;
        try {
            CloseableHttpClient httpClient = getHttpClient();
            StringEntity stringEntity = new StringEntity(payload);
            httpPost.setEntity(stringEntity);
            httpResponse = httpClient.execute(httpPost);
            return parseResponse(httpResponse, type);
        } catch (IOException e) {
            if (logger.isErrorEnabled()) {
                logger.error("http request error", e);
            }
        } finally {
            try {
                //执行httpResponse.close关闭对象会关闭连接池，
                //如果需要将连接释放到连接池，可以使用EntityUtils.consume()方法
                if (httpResponse != null) {
                    EntityUtils.consume(httpResponse.getEntity());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private static <T> T parseResponse(CloseableHttpResponse httpResponse, TypeReference<T> type) throws IOException {
        if(HttpStatus.SC_OK == httpResponse.getStatusLine().getStatusCode()){
            String content = EntityUtils.toString(httpResponse.getEntity());
            if (type == null) {
                return (T) content;
            }
            return toBean(content, type);
        }
        return null;
    }

    public static <T> T toBean(String jsonStr, TypeReference<T> typeReference) throws JsonProcessingException {
        return MAPPER.readValue(jsonStr, typeReference);
    }
}
