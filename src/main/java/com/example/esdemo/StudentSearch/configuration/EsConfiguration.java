package com.example.esdemo.StudentSearch.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class EsConfiguration {

  @Value("${spring.data.elasticsearch.cluster-nodes}")
  private String clusterNodes;

  @Value("${spring.data.elasticsearch.cluster-name}")
  private String clusterName;

  private RestHighLevelClient restHighLevelClient;

  @Bean
  public RestHighLevelClient createInstance() {
    return buildClient();
  }

  private RestHighLevelClient buildClient() {
    try {
      restHighLevelClient = new RestHighLevelClient(RestClient.builder(
                  new HttpHost("localhost", 9200, "http"),
                  new HttpHost("localhost", 9201, "http")));
    }
    catch (Exception e) {
      log.error(e.getMessage());
      throw e;
    }
    return restHighLevelClient;
  }
}
