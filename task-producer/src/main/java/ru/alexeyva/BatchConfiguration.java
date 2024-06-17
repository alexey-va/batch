package ru.alexeyva;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobLocator;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.ListableJobLocator;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Configuration
@RequiredArgsConstructor
public class BatchConfiguration {

    final DataSource batchDataSource;

    @Bean
    JobRepository jobRepository() throws Exception {
        var factory = new JobRepositoryFactoryBean();
        factory.setDataSource(batchDataSource);

        DefaultLobHandler lobHandler = new DefaultLobHandler();
        lobHandler.setCreateTemporaryLob(false);
        factory.setLobHandler(lobHandler);

        factory.setTransactionManager(transactionManager());
        factory.afterPropertiesSet();
        return factory.getObject();
    }

    @Bean
    PlatformTransactionManager transactionManager() {
        return new DataSourceTransactionManager(batchDataSource);
    }

    @Bean
    JobLauncher jobLauncher() throws Exception {
        var jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    @Bean
    JobExplorer jobExplorer() throws Exception {
        var factory = new JobExplorerFactoryBean();
        factory.setDataSource(batchDataSource);
        factory.afterPropertiesSet();
        return factory.getObject();
    }

    @Bean
    MapJobRegistry jobRegistry() {
        return new MapJobRegistry();
    }

    @Bean
    JobBuilderFactory jobBuilderFactory() throws Exception {
        return new JobBuilderFactory(jobRepository());
    }

    @Bean
    StepBuilderFactory stepBuilderFactory() throws Exception {
        return new StepBuilderFactory(jobRepository(), transactionManager());
    }

    @Bean
    FlatFileItemReader<Task> reader() {
        return new FlatFileItemReaderBuilder<Task>()
                .name("taskItemReader")
                .resource(new ClassPathResource("tasks.csv"))
                .maxItemCount(1)
                .delimited()
                .names(new String[]{"name", "status", "result"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(Task.class);
                }})
                .build();
    }

    @Bean
    ItemWriter<Task> writer() throws Exception {
        final Path path = Path.of("output.txt");
        Files.deleteIfExists(path);
        return list -> {
            if (Files.notExists(path)) Files.createFile(path);
            for (Task task : list) {
                Files.write(path, (task.toString() + "\n").getBytes(), StandardOpenOption.APPEND);
            }
        };
    }

    @Bean
    ItemProcessor<Task, Task> processor() {
        return new ItemProcessor<>() {
            int count = 0;
            @Override
            public Task process(Task task) throws Exception {
                System.out.println("Processing task: " + task+" count "+count+" current second "+ LocalTime.now().getSecond() +" on thread "+Thread.currentThread().getName());

                if (count++ < 2) throw new Exception("Random exception");
                task.setStatus(Task.Status.DONE);
                task.setResult("Processed");
                count=0;
                return task;
            }
        };
    }

    @Bean
    TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor("spring_batch");
    }

    @Bean
    Step step1() throws Exception {
        var backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(2000);
        backOffPolicy.setMultiplier(2);
        return stepBuilderFactory()
                .get("retryStep")
                .<Task, Task>chunk(1)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .faultTolerant()
                .backOffPolicy(backOffPolicy)
                .retryLimit(3)
                .retry(Throwable.class)
                .taskExecutor(taskExecutor())
                .throttleLimit(1)
                .build();
    }

    @Bean
    Job job() throws Exception {
        return jobBuilderFactory()
                .get("main_job")
                .start(step1())
                .build();
    }


}
