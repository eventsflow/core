
from eventsflow.workers.process import ProcessingWorker


class SampleProcessingWorker(ProcessingWorker):
    def process(self, event):
        self.publish(event)
        return True
