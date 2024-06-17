package ru.alexeyva;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ThreadLocalRandom;

@RestController
@RequiredArgsConstructor
public class Controller {

    final JobLauncher jobLauncher;
    final Job job;

    @GetMapping("/launch/{id}")
    public String launch(@PathVariable(name = "id") int id) {
        try {
            jobLauncher
                    .run(job, new JobParametersBuilder()

                    .addLong("time", System.currentTimeMillis())
                    .addString("random", String.valueOf(ThreadLocalRandom.current().nextInt()))
                    .toJobParameters());
        } catch (Exception e) {
            return "Failed";
        }
        return "Launched";
    }

}
