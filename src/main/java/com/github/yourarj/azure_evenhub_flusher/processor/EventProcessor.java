package com.github.yourarj.azure_evenhub_flusher.processor;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;


public class EventProcessor implements IEventProcessor {
    public static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);
    private StopWatch stopWatch = new StopWatch();

    @Override
    public void onOpen(PartitionContext ctx) throws Exception {
        LOGGER.info("Event processor#: {} of consumer group {} initialized to receive message from partitionID#: {}",
                ctx.getOwner(),
                ctx.getConsumerGroupName(),
                ctx.getPartitionId());
        stopWatch.start();
    }

    @Override
    public void onClose(PartitionContext ctx, CloseReason reason) throws Exception {
        LOGGER.info("Event processor#: {} stopped processing messages from partitionID#: {}\n" +
                        "Reason: {}",
                ctx.getOwner(),
                ctx.getPartitionId(),
                reason);

        ctx.checkpoint();
        stopWatch.stop();
    }

    @Override
    public void onEvents(PartitionContext ctx, Iterable<EventData> events) throws Exception {

        if(stopWatch.getTotalTimeSeconds()>30){
            ctx.checkpoint();
            stopWatch.stop();
            stopWatch.start();
        }
    }

    @Override
    public void onError(PartitionContext ctx, Throwable error) {
        LOGGER.error("Event processor#: {} of consumer group {} consuming partitionID#: {} encountered an Exception\n" +
                        "Error Details: {}",
                ctx.getOwner(),
                ctx.getConsumerGroupName(),
                ctx.getPartitionId(),
                error.getMessage());
        stopWatch.stop();
    }
}
