##1.0-RELEASE
   - Added message producing capability, enable `producer` spring profile for the same
   - Now it's mandatory activate `consumer` spring profile to act app as consumer
   - Renamed property `app.event-hub.consumer-group` to  `app.event-hub.consumer.consumer-group`
   - Renamed property `app.event-hub.batch-size` to `app.event-hub.consumer.batch-size`
   - Renamed property `app.event-hub.prefetch-count` to `app.event-hub.consumer.prefetch-count`
   - Azure Event Hub Event Processor Host version bumped to latest `2.5.0`