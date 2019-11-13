package com.github.yourarj.azure_evenhub_flusher.producer;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import lombok.Data;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.CompletableFuture;

@Component
@EnableScheduling
@Profile("producer")
class EventHubProducer {
    private static final int EVENT_HUB_CLIENT_COUNT = 10;
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHubProducer.class);
    private Random random = new Random();
    private final EventHubClient[] clients = new EventHubClient[EVENT_HUB_CLIENT_COUNT];

    private EventHubProducer(BeanFactory beanFactory) {
        for (int i = 0; i < EVENT_HUB_CLIENT_COUNT; i++) {
            clients[i] = beanFactory.getBean(EventHubClient.class);
        }
    }

    @Scheduled(fixedRateString = "${app.event-hub.producer.fixed-rate}")
    public void sendMessage(){
        for (int i = 0; i < 10; i++) {
            clients[random.nextInt(EVENT_HUB_CLIENT_COUNT)]
                    .send(EventData.create(RandomStringUtils.random(1000).getBytes()))
                    .exceptionally(throwable -> {LOGGER.error(throwable.getMessage()); return null;});
        }
    }
}