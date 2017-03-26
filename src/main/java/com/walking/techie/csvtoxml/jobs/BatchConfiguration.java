package com.walking.techie.csvtoxml.jobs;

import com.walking.techie.csvtoxml.model.Student;
import com.walking.techie.csvtoxml.processor.StudentProcessor;
import java.util.HashMap;
import java.util.Map;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

  @Autowired
  private JobBuilderFactory jobBuilderFactory;
  @Autowired
  private StepBuilderFactory stepBuilderFactory;


  @Bean
  public Job CsvToXmlJob() {
    return jobBuilderFactory.get("CsvToXmlJob").flow(step1()).end().build();
  }

  @Bean
  public Step step1() {
    return stepBuilderFactory.get("step1").<Student, Student>chunk(10).reader(reader())
        .writer(writer()).processor(processor()).build();
  }

  @Bean
  public StudentProcessor processor() {
    return new StudentProcessor();
  }

  @Bean
  public FlatFileItemReader<Student> reader() {
    FlatFileItemReader<Student> reader = new FlatFileItemReader<>();
    reader.setResource(new ClassPathResource("student.csv"));
    reader.setLineMapper(new DefaultLineMapper<Student>() {{
      setFieldSetMapper(new BeanWrapperFieldSetMapper<Student>() {{
        setTargetType(Student.class);
      }});
      setLineTokenizer(new DelimitedLineTokenizer() {{
        setNames(new String[]{"rollNo", "name", "department"});
      }});
    }});
    return reader;
  }

  @Bean
  public StaxEventItemWriter<Student> writer() {
    StaxEventItemWriter<Student> writer = new StaxEventItemWriter<>();
    writer.setResource(new FileSystemResource("xml/student.xml"));
    writer.setMarshaller(studentUnmarshaller());
    writer.setRootTagName("students");
    return writer;
  }

  @Bean
  public XStreamMarshaller studentUnmarshaller() {
    XStreamMarshaller unMarshaller = new XStreamMarshaller();
    Map<String, Class> aliases = new HashMap<String, Class>();
    aliases.put("student", Student.class);
    unMarshaller.setAliases(aliases);
    return unMarshaller;
  }
}
