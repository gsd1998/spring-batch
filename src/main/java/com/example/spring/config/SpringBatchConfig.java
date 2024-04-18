package com.example.spring.config;


import com.example.spring.model.Customer;
import com.example.spring.partitionhandler.ColumnRangePartitioner;
import com.example.spring.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.File;

@Configuration
@AllArgsConstructor
public class SpringBatchConfig {

    private JobRepository jobRepository;

    private PlatformTransactionManager transactionManager;

    //private CustomerRepository customerRepository;

    private CustomerWriter customerWriter;

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> itemReader(@Value("#{jobParameters[fullFilePath]}") String filePath) {

        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setName("csvReader");
        //itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setResource(new FileSystemResource(new File(filePath)));
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());

        return itemReader;
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    @Bean
    public CustomerItemProcessor processor(){
        return new CustomerItemProcessor();
    }

    @Bean
    public ColumnRangePartitioner partitioner(){
        return new ColumnRangePartitioner();
    }

    @Bean
    public PartitionHandler partitionHandler( FlatFileItemReader<Customer> itemReader){
        TaskExecutorPartitionHandler partitionHandler =  new TaskExecutorPartitionHandler();
        partitionHandler.setGridSize(4);
        partitionHandler.setTaskExecutor(taskExecutor());
        partitionHandler.setStep(slaveStep(itemReader));

        return partitionHandler;
    }

    @Bean
    public Step slaveStep(FlatFileItemReader<Customer> itemReader){
        return new StepBuilder("slaveStep",jobRepository)
                .<Customer,Customer>chunk(250,transactionManager)
                .reader(itemReader)
                .processor(processor())
                .writer(customerWriter)
                .build();
    }

    @Bean
    public Step masterStep(FlatFileItemReader<Customer> itemReader){
        return new StepBuilder("masterStep",jobRepository)
                .partitioner(slaveStep(itemReader).getName(),partitioner())
                .partitionHandler(partitionHandler(itemReader))
                .build();
    }

    @Bean
    public Job runJob(FlatFileItemReader<Customer> itemReader){
        return new JobBuilder("importCustomers",jobRepository)
                .flow(masterStep(itemReader))
                .end().build();
    }


    @Bean
    public TaskExecutor taskExecutor(){
        //SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        //asyncTaskExecutor.setConcurrencyLimit(10);

        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setQueueCapacity(4);

        return taskExecutor;
    }


}
