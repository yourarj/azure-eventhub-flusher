# Azure Event Hub Flusher / Producer
Sometimes we require an eventhub to be purged, or sending bulk messages to Event Hub. 
In that case we can use this tool which simply iterates through the eventhub messages and  
checkpoints to latest messages for provided consumer group.  
Technically, it does not remove any messages from Event Hub, just updates checkpoints for provided consumer group.

###Prerequisites:
- Spring profiles `producer` or `consumer`, or maybe both
    - we need to provide either of `consumer` or `producer` spring profile. Or if required we can enable both.

- System Properties / Environment variables
   - `app.event-hub.partition-count` - Event Hub queue partition count
     - alias `EVPC`
   - `app.event-hub.connection-string` - Event Hub connection string
     - alias `EVCS`
   - `app.event-hub.storage.connection-string` - Event Hub Storage connection string
     - alias `EVSCS`
   - `app.event-hub.storage.container-name` - Event Hub Storage container name
     - alias `EVSCN`
   - `app.event-hub.consumer.consumer-group` - Event Hub queue consumer group name
     - alias `EVCG`
   - `app.event-hub.consumer.batch-size` - Event Hub consumer batch size
     - `default - 1998`
   - `app.event-hub.consumer.prefetch-count` - Event Hub prefetch count
     - `default - 1999`
   - `app.event-hub.producer.fixed-rate` - Event Hub Producer sending per millisecond
     - `default - 1` i.e. 1000 messages are sent per second

 **_Thanks and Regards,_**
###Mr. Awesome