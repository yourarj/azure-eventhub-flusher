# Azure Event Hub Flusher
Sometimes we require an eventhub to be purged.  
In that case we can use this tool which simply iterates through the eventhub messages and  
checkpoints to latest messages for provided consumer group.  
It doesn't literray flushes out messages of a event hub

###Prerequisites:
* Environment variables
    1. **EVCS** - Azure Event Hub Connection String
    2. **CG** - Consumer Group Name
    3. **SCS** - Azure Storage Connection String
    4. **SCN** - Azure Storage Connection String

 **_Thanks and Regards,_**
###Mr. Awesome