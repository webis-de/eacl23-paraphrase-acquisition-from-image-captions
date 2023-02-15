import abc


class Scorer(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def score(self, paraphrase, first_tokens, second_tokens):
        raise NotImplementedError()


