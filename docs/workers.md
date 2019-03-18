# Workers

The workers perform main data processing logic: reading and writing file, data fetching from end points, payload transformation, filtering, events routing etc.

The workers can be different types:
- simple worker, started as separate process
- async worker, the same as simple but it has async nature for processing events

## Configuration

Simple form of worker's configuration:
```yaml
- name: TestWorker
  type: eventsflow.workers.process.ProcessingWorker
  description: Test worker
  instances: 1
  parameters: 
    param1: values1
    param2: values2
  input: input-queue
  output: output-queue
```

where

| Parameter   | Description                                                  |
| ----------- | ------------------------------------------------------------ |
| name        | the worker name                                              |
| type        | the worker type                                              |
| description | the short description, to explain shortly the main logic of the worker |
| instances   | the number of worker instances which need to create for processing |
| parameters  | the key/value pairs for specifying worker specific parameters, like access to database: hostname, username, passwors, connection settings, etc |
| input       | the short annotation for input queue                         |
| output      | the short annotation for output queue                        |

