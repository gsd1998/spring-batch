package com.example.spring.controller;


import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;

@RestController
public class CustomerController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    private final String TEMP_STORAGE ="C:\\Users\\girid\\Desktop\\batch\\";

    @PostMapping(path = "/import")
    public void importCsvToDb(@RequestParam("file") MultipartFile multipartFile){

        try {
            String original_name = multipartFile.getOriginalFilename();
            File filename = new File(TEMP_STORAGE + original_name);
            multipartFile.transferTo(filename);

            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("fullFilePath", TEMP_STORAGE + original_name)
                    .addLong("startAt",System.currentTimeMillis()).toJobParameters();
            JobExecution execution =jobLauncher.run(job,jobParameters);
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException |
                 JobParametersInvalidException | IOException e) {
            e.printStackTrace();
        }
    }

}
