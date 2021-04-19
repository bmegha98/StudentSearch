package com.example.esdemo.StudentSearch.controller;

import com.example.esdemo.StudentSearch.model.Student;
import com.example.esdemo.StudentSearch.service.StudentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/student")
public class StudentController {
  @Autowired
  private StudentService studentService;

  @PostMapping("/ingest")
  public HttpStatus ingestData(@RequestBody Student student) throws IOException {
    if(studentService.ingestData(student))
      return HttpStatus.OK;
    return HttpStatus.BAD_REQUEST;
  }

  @PostMapping("/bulkIngest")
  public HttpStatus bulkIngestData() throws IOException, InterruptedException {
    return studentService.bulkIngest();
  }

  @GetMapping
  public List<Student> getAllStudents() throws IOException {
    return studentService.getAllStudents();
  }

  @GetMapping("/search/{searchString}")
  public List<Student> getMatchedStudents(@PathVariable String searchString) throws IOException {
    return studentService.getMatchedStudents(searchString);
  }
}
