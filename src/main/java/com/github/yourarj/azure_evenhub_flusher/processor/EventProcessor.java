package com.github.yourarj.azure_evenhub_flusher.processor;

import com.google.gson.JsonObject;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import jdk.nashorn.internal.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;


public class EventProcessor implements IEventProcessor {
    private static final AtomicLong processedMessages = new AtomicLong(0);
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);
    private Long processed = 0L;
    private long startTimeMillis = System.currentTimeMillis();
    private final int pauseInterval = 1000;
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @Override
    public void onOpen(PartitionContext ctx) {
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Event processor#: {} of consumer group {} initialized to receive message from partitionID#: {}",
                    ctx.getOwner(),
                    ctx.getConsumerGroupName(),
                    ctx.getPartitionId());
        startTimeMillis = System.currentTimeMillis();
    }

    @Override
    public void onClose(PartitionContext ctx, CloseReason reason) {
        LOGGER.info("Event processor#: {} stopped processing messages from partition ID: {}\n" +
                        "Reason: {}",
                ctx.getOwner(),
                ctx.getPartitionId(),
                reason);

        if (!reason.equals(CloseReason.LeaseLost))
            this.checkpoint(ctx);
        startTimeMillis = System.currentTimeMillis();
    }

    @Override
    public void onEvents(PartitionContext ctx, Iterable<EventData> events) {

        long eventCount = StreamSupport.stream(events.spliterator(), Boolean.TRUE).count();


//        StreamSupport.stream(events.spliterator(), Boolean.TRUE).forEach(eventData -> LOGGER.info(new String(eventData.getBytes())));
        processed += eventCount;

        if (processed > 100) {
            processedMessages.addAndGet(processed);
            processed = 0L;
            LOGGER.info("Total processed messages: {}", processedMessages.get());
        }

        if (LOGGER.isDebugEnabled()) {
            executorService.submit(() -> {

                LOGGER.debug("Received EventData batch with {} events", eventCount);

                Optional<EventData> max = StreamSupport.stream(events.spliterator(), Boolean.TRUE)
                        .max(Comparator.comparing(eventData -> eventData.getSystemProperties().getOffset()));
                max.ifPresent(
                        eventData -> LOGGER.debug("Offset: {} Sequence#: {}\n",
                                eventData.getSystemProperties().getOffset(),
                                eventData.getSystemProperties().getSequenceNumber())
                );
            });
        }

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
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("###### Check pointing partition ID: {} - Event processor: {}", ctx.getPartitionId(), ctx.getOwner());
        try {
            ctx.checkpoint().get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Error while check pointing. {}", e.getMessage());
        }
    }
}