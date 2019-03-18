# EventsFlow

Simple library for building data processing pipelines and control them via console tool. 

Where eventsflow library can be used:

- data ingestion
- data validation
- data trasformation and filtering
- data migration between different formats, databases and storages

Eventsflow limits data processing only to one single node. The components listed below are building blocks for data processing with eventsflow

- [Events](events.html)
- [Workers](workers.html)
- [Queues](queues.html)
- [Flow](flow.html)

## Events

The events are containers for data transporting  between workers. Each event contains 3 fields: event name, metadata and payload. For more details, please read [Events spec](events.html)

## Workers

The workers perform main data processing logic: reading and writing file, data fetching from end points, payload transformation, filtering, events routing etc. For more details, please read [Workers spec](workers.html)

## Queues

to be described later

## Flow

to be described later


