package com.github.yourarj.azure_evenhub_flusher.processor;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import java.util.Comparator;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class EventProcessor implements IEventProcessor {
    public static AtomicLong processedMessages = new AtomicLong(0);
    public static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);
    private Long processed = 0L;
    private long startTimeMillis = System.currentTimeMillis();
    private final int pauseInterval = 10000;
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @Override
    public void onOpen(PartitionContext ctx) throws Exception {
        LOGGER.info("Event processor#: {} of consumer group {} initialized to receive message from partitionID#: {}",
                ctx.getOwner(),
                ctx.getConsumerGroupName(),
                ctx.getPartitionId());
        startTimeMillis = System.currentTimeMillis();
    }

    @Override
    public void onClose(PartitionContext ctx, CloseReason reason) throws Exception {
        LOGGER.info("Event processor#: {} stopped processing messages from partition ID: {}\n" +
                        "Reason: {}",
                ctx.getOwner(),
                ctx.getPartitionId(),
                reason);

        this.checkpoint(ctx);
        startTimeMillis = System.currentTimeMillis();
    }

    @Override
    public void onEvents(PartitionContext ctx, Iterable<EventData> events) throws Exception {

        long eventCount = StreamSupport.stream(events.spliterator(), Boolean.TRUE).count();
        processed += eventCount;

        if (eventCount > 1000) {
            processedMessages.addAndGet(eventCount);
            processed = 0L;
            LOGGER.info("Total processed messages: {}", processedMessages.get());
        }

        executorService.submit(() -> {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Received EventData batch with {} events", eventCount);

                Optional<EventData> max = StreamSupport.stream(events.spliterator(), Boolean.TRUE)
                        .max(Comparator.comparing(eventData -> eventData.getSystemProperties().getOffset()));
                max.ifPresent(
                        eventData -> LOGGER.debug("Offset: {} Sequence#: {}\n",
                                eventData.getSystemProperties().getOffset(),
                                eventData.getSystemProperties().getSequenceNumber())
                );
            }
        });

        if (System.currentTimeMillis() - startTimeMillis > pauseInterval) {
            this.checkpoint(ctx);
            startTimeMillis = System.currentTimeMillis();
        }
    }

    @Override
    public void onError(PartitionContext ctx, Throwable error) {
        LOGGER.error("Event processor#: {} of consumer group {} consuming partition ID: {} encountered an Exception\n" +
                        "Error Details: {}",
                ctx.getOwner(),
                ctx.getConsumerGroupName(),
                ctx.getPartitionId(),
                error.getMessage());
        startTimeMillis = System.currentTimeMillis();
    }

    private void checkpoint(PartitionContext ctx) {
        LOGGER.info("###### Check pointing partition ID: {} - Event processor: {}", ctx.getPartitionId(), ctx.getOwner());
        try {
            ctx.checkpoint().get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Error while check pointing. {}", e.getMessage());
        }
    }
}
