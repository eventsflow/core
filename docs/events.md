# Events

The events are containers for data transporting  between workers. Each event contains 3 fields: event name, metadata and payload.

The event format
```json
{
    "name":     "<EventName>",
    "metadata": { <event metadata as key/value pairs> },
    "payload":  [ <payload>, ]
}
```

## Event: StopProcessing

The events can be used not only for data transport but inform workers about status or processing changes. For instance, the producer do not have any data for processing and inform about it consumer, the event `EventStopProcessing` is used to inform the consumer to stop processing when it received the event. 


