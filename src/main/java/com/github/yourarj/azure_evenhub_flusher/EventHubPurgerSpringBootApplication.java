package com.github.yourarj.azure_evenhub_flusher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EventHubPurgerSpringBootApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventHubPurgerSpringBootApplication.class, args);
    }
}
