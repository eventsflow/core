# Workers

The workers perform main data processing logic: reading and writing file, data fetching from end points, payload transformation, filtering, events routing etc.

The workers can be different types:
- simple worker, started as separate process
- async worker, the same as simple but it has async nature for processing events

## ProcessingWorker

```python

worker = ProcessingWorker(name)
```