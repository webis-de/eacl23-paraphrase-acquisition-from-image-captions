from nltk.translate import bleu_score

from paraphrase.scorer.scorer import Scorer


class BleuScorer(Scorer):
    def __init__(self, n=4):
        self.n = n
        self.smoothing = bleu_score.SmoothingFunction()

    def score(self, paraphrase, first_tokens, second_tokens):
        paraphrase.similarities["bleu"] = \
            (bleu_score.sentence_bleu([first_tokens], second_tokens, smoothing_function=self.smoothing.method7) +
             bleu_score.sentence_bleu([second_tokens], first_tokens, smoothing_function=self.smoothing.method7)) / 2.0

        paraphrase.similarities["bleu"] = max(0, paraphrase.similarities["bleu"])
        paraphrase.similarities["bleu"] = min(1, paraphrase.similarities["bleu"])
