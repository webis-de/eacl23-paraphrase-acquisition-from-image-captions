import math

from paraphrase.scorer.scorer import Scorer


class WordMoverDistanceScorer(Scorer):
    def __init__(self):
        from nltk.corpus import stopwords
        from nltk import download
        import gensim.downloader as gensim_api
        download("stopwords")
        self.stop_words = stopwords.words("english")
        self.model = gensim_api.load("fasttext-wiki-news-subwords-300")

    def score(self, paraphrase, first_tokens, second_tokens):
        first = [w for w in first_tokens if w not in self.stop_words]
        second = [w for w in second_tokens if w not in self.stop_words]

        paraphrase.similarities["wms"] = 1.0 - (self.model.wmdistance(first, second) / 2.0)
        if math.isinf(paraphrase.similarities["wms"]):
            paraphrase.similarities["wms"] = 0


