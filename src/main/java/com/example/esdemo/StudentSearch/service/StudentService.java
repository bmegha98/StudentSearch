package com.example.esdemo.StudentSearch.service;

import com.example.esdemo.StudentSearch.model.Student;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Slf4j
@Service
public class StudentService {
  @Autowired private RestHighLevelClient client;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private BulkIngestService bulkIngestService;

  private long totalStudents() throws IOException {
    CountRequest countRequest = new CountRequest("student_index");
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.matchAllQuery());
    countRequest.source(sourceBuilder);

    CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
    return countResponse.getCount();
  }

  public Boolean ingestData(Student studentInfo) throws IOException {
    long countOfStudents = this.totalStudents();
    /*
    Document source provided as an XContentBuilder object, the Elasticsearch built-in helpers to generate
     JSON content
     */
    studentInfo.setRollNumber(countOfStudents+1);
    Map<String, Object> studentMap = objectMapper.convertValue(studentInfo, HashMap.class);
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
    IndexRequest indexRequest = new IndexRequest("student_index", "_doc", studentInfo.getRollNumber().toString());
    indexRequest.source(builder);
    try {
      client.index(indexRequest, RequestOptions.DEFAULT);
    }
    catch (ElasticsearchException exception) {
      log.error("Error while indexing the document " ,exception);
      return false;
    }
    log.info("Data ingested successfully ! ");
    return true;
  }

  public List<Student> getMatchedStudents(String searchString) throws IOException {
    List<Student> matchedStudents = new ArrayList<>();
    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
    boolQueryBuilder.should(QueryBuilders.termQuery("Gender", searchString));
    boolQueryBuilder.should(QueryBuilders.termQuery("Age", searchString));
    boolQueryBuilder.should(QueryBuilders.termQuery("RollNumber", searchString));
    boolQueryBuilder.should(QueryBuilders.matchQuery("FirstName", searchString).boost(0.4f)
      .fuzziness(Fuzziness.AUTO));
    boolQueryBuilder.should(QueryBuilders.matchQuery("LastName", searchString).boost(0.4f)
      .fuzziness(Fuzziness.AUTO));
    boolQueryBuilder.should(QueryBuilders.matchQuery("Address", searchString).boost(0.3f)
      .fuzziness(Fuzziness.AUTO));

    SearchRequest searchRequest = new SearchRequest("student_index");
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(boolQueryBuilder);
    searchRequest.source(sourceBuilder);

    SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
    SearchHit[] hits = searchResponse.getHits().getHits();

    for (SearchHit searchHit : hits)
      matchedStudents.add(objectMapper.convertValue(searchHit.getSourceAsMap(), Student.class));

    return matchedStudents;
  }

  public List<Student> getAllStudents() throws IOException {
    List<Student> allStudents = new ArrayList<>();
    SearchRequest searchRequest = new SearchRequest("student_index");
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.matchAllQuery());
    sourceBuilder.from(0);
    sourceBuilder.size(25);
    searchRequest.source(sourceBuilder);

    SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
    SearchHit[] hits = searchResponse.getHits().getHits();

    for (SearchHit searchHit : hits)
      allStudents.add(objectMapper.convertValue(searchHit.getSourceAsMap(), Student.class));

    return allStudents;
  }

  public HttpStatus bulkIngest() throws IOException, InterruptedException {
    File jsonFile = new File("/home/meghabisht/Downloads/Students.json");
    log.info(String.valueOf(jsonFile));
    FileReader fr = new FileReader(jsonFile);
    BufferedReader bufferedReader = new BufferedReader(fr);

    List<Student> studentList = new ArrayList<>();
    String line = null;
    long countOfStudents = this.totalStudents();
    Integer count = 1;

    while((line = bufferedReader.readLine()) != null) {
      Student student = objectMapper.readValue(line, Student.class);
      student.setRollNumber(countOfStudents+count);
      count++;
      studentList.add(student);
    }

    bulkIngestService.ingestData(studentList);
    return HttpStatus.OK;
  }
}
