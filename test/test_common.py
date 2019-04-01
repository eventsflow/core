
import logging

from eventsflow.workers.process import ProcessingWorker


logger = logging.getLogger(__name__)


class SampleProcessingWorker(ProcessingWorker):
    def process(self, event):
        yield event

