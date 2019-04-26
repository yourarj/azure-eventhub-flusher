package com.github.yourarj.azure_evenhub_flusher.producer;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@EnableScheduling
@Profile("producer")
public class EventHubProducer {
    String string  = "";
    public static final Logger LOGGER = LoggerFactory.getLogger(EventHubProducer.class);
    private final EventHubClient eventHubClient;

    public EventHubProducer(EventHubClient eventHubClient) {
        this.eventHubClient = eventHubClient;
    }

    @Scheduled(fixedRateString = "${app.event-hub.producer.fixed-rate}")
    public void sendMessage(){
        CompletableFuture<Void> send = eventHubClient.send(EventData.create(RandomStringUtils.random(100).getBytes()));
        send.thenRun(() ->{if(LOGGER.isDebugEnabled())LOGGER.debug("Message sent successfully");});
        send.exceptionally(throwable -> {LOGGER.error(throwable.getMessage()); return null;});
    }
}
