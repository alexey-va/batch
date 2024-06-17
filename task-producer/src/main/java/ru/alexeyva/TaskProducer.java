package ru.alexeyva;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TaskProducer {
    public static void main(String[] args) {
        System.out.println(1);
        SpringApplication.run(TaskProducer.class, args);
    }
}