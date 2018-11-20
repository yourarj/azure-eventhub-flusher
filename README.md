# azure-eventhub-flusher
Sometimes we require an eventhub to be purged. In that case we can use this tool which simply iterates through the eventhub messages and checkpoints to latest messages for provided consumer group.
it doesn't literray flushes out messages of a event hub
