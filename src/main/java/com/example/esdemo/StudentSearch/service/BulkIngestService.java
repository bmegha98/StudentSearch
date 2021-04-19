package com.example.esdemo.StudentSearch.service;

import com.example.esdemo.StudentSearch.model.Student;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
@Slf4j
public class BulkIngestService {
  @Autowired private RestHighLevelClient client;
  @Autowired private ObjectMapper objectMapper;

  public void ingestData(List<Student> studentList) throws InterruptedException {
    BulkProcessor.Listener listener = new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
        log.info("Ingestion started");
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                            BulkResponse response) {
        if (response.hasFailures()) {
          for (BulkItemResponse bulkItemResponse : response) {
            if (bulkItemResponse.isFailed()) {
              BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
              log.info("Failed Operation {}: ",bulkItemResponse.getOpType(), failure.getCause());
            }
          }
        }
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                            Throwable failure) {
        log.error("Error occurred ", failure);
      }
    };

    BulkProcessor bulkProcessor = BulkProcessor.builder((request, bulkListener) ->
      client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener).build();

    try {
      studentList.forEach(student -> {
        Map<String, Object> studentMap = objectMapper.convertValue(student, HashMap.class);
        Set<String> keys = studentMap.keySet();
        XContentBuilder builder = null;
        try{
          builder = jsonBuilder().startObject();
          for(String key : keys){
            builder.field(key, studentMap.get(key));
          }
          builder.endObject();
        }
        catch (IOException e) {
          e.printStackTrace();
        }

        IndexRequest indexRequest = new IndexRequest("student_index", "_doc", student.getRollNumber().toString());
        indexRequest.source(builder);

        bulkProcessor.add(indexRequest);
      });
    }
    catch (Exception e) {
      log.error("error encountered", e);
      throw e;
    }

    //Once all requests have been added to the BulkProcessor, its instance needs to be closed.
    try {
      boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
      log.info("Bulk Data Ingestion Success  {}", terminated);
    }
    catch (InterruptedException e) {
      log.error("Error encountered", e);
      throw e;
    }
  }
}
