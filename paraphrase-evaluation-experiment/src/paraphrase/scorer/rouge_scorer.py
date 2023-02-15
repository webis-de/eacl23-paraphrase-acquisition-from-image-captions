from rouge_score import rouge_scorer

from paraphrase.scorer.scorer import Scorer


class RougeScorer(Scorer):
    def __init__(self):
        self.scorer = rouge_scorer.RougeScorer(["rouge1", "rougeL"], use_stemmer=True)

    def score(self, paraphrase, first_tokens, second_tokens):
        rouge = self.scorer.score(paraphrase.first_entry.text.lower(), paraphrase.second_entry.text.lower())
        paraphrase.similarities["rouge1-precision"] = rouge["rouge1"].precision
        paraphrase.similarities["rouge1-recall"] = rouge["rouge1"].recall
        paraphrase.similarities["rouge1-f1"] = rouge["rouge1"].fmeasure
        paraphrase.similarities["rougel-precision"] = rouge["rougeL"].precision
        paraphrase.similarities["rougel-recall"] = rouge["rougeL"].recall
        paraphrase.similarities["rougel-f1"] = rouge["rougeL"].fmeasure
