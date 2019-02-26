package com.github.yourarj.azure_evenhub_flusher.config;


import com.github.yourarj.azure_evenhub_flusher.processor.EventProcessor;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventPosition;
import com.microsoft.azure.eventhubs.ReceiverRuntimeInformation;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import com.microsoft.azure.eventprocessorhost.IEventProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import javax.annotation.PostConstruct;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Configuration
public class EventHubConfig {

    public static final Logger LOGGER = LoggerFactory.getLogger(EventHubConfig.class);

    @Value("${app.event-hub.connection-string}")
    private String connectionString;

    @Value("${app.event-hub.consumer-group}")
    private String consumerGroup;

    @Value("${app.event-hub.storage.connection-string}")
    private String storageConnectionString;

    @Value("${app.event-hub.storage.container-name}")
    private String storageContainerName;

    @Value("${app.event-hub.batch-size}")
    private int batchSize;

    @Value("${app.event-hub.prefetch-count}")
    private int prefetchCount;

    @Value("${app.event-hub.partition-count}")
    private int partitionCount;

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public EventProcessorHost eventProcessorHost() {
        ConnectionStringBuilder builder = new ConnectionStringBuilder(connectionString);
        return new EventProcessorHost(
                UUID.randomUUID().toString(),
                builder.getEventHubName(),
                consumerGroup,
                builder.toString(),
                storageConnectionString,
                storageContainerName
        );
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public IEventProcessorFactory<EventProcessor> eventProcessor() {

        IEventProcessorFactory<EventProcessor> factory = partitionContext -> {
            ReceiverRuntimeInformation runtimeInformation = partitionContext.getRuntimeInformation();
            LOGGER.debug("Creating event processor of Consumer Group: {}\n" +
                            "for partitionId: {}\n" +
                            "Last Enqueued Offset: {}" +
                            "Last Enqueued Sequence Number: {}" +
                            "Last Enqueued Time: {}" +
                            "Retrieval Time: {}",
                    partitionContext.getConsumerGroupName(),
                    partitionContext.getPartitionId(),
                    runtimeInformation.getLastEnqueuedOffset(),
                    runtimeInformation.getLastEnqueuedSequenceNumber(),
                    runtimeInformation.getLastEnqueuedTime(),
                    runtimeInformation.getRetrievalTime()
            );
            return new EventProcessor();
        };
        return factory;
    }

    @PostConstruct
    public void afterPropertiesSet() throws ExecutionException, InterruptedException {

        EventProcessorOptions options = new EventProcessorOptions();
        options.setMaxBatchSize(batchSize);
        options.setPrefetchCount(prefetchCount);
        options.setInitialPositionProvider(s -> {
            LOGGER.info("String from setInitialPositionProvider: {}",s);
            return EventPosition.fromOffset(s);
        });

        for (int i = 0; i < partitionCount; i++) {

            EventProcessorHost eventProcessorHost = eventProcessorHost();
            IEventProcessorFactory<EventProcessor> factory = eventProcessor();
            LOGGER.info("Registering EventProcessor#{}...",i);
            CompletableFuture<Void> completableFuture = eventProcessorHost.registerEventProcessorFactory(factory, options);

            completableFuture.thenAccept(aVoid -> LOGGER.info("EventProcessor registered successfully!"));

            completableFuture.exceptionally(throwable -> {
                LOGGER.error("EventProcessor registration failed\n" +
                                "reason: {}",
                        throwable.getMessage());
                return null;
            });

            completableFuture.get();
        }
    }
}
