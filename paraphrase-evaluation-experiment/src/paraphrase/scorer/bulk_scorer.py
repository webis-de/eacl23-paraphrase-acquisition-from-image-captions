import abc


class BulkScorer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def bulk_score(self, paraphrases):
        pass
