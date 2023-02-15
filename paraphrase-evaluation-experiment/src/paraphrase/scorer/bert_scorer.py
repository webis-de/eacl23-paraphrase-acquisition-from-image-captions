from paraphrase.scorer.scorer import Scorer


class BertScorer(Scorer):
    def __init__(self, *args):
        from bert_score import BERTScorer
        self.scorer = BERTScorer(lang="en", rescale_with_baseline=True)

    def score(self, paraphrase, first_tokens, second_tokens):
        import transformers
        import logging

        transformers.tokenization_utils.logger.setLevel(logging.ERROR)
        transformers.configuration_utils.logger.setLevel(logging.ERROR)
        transformers.modeling_utils.logger.setLevel(logging.ERROR)

        first_texts = [paraphrase.first_entry.text.lower()]
        second_texts = [paraphrase.second_entry.text.lower()]

        precision, recall, f1 = self.scorer.score(first_texts, second_texts)

        paraphrase.similarities["bert-score-precision"] = precision[0].item()
        paraphrase.similarities["bert-score-recall"] = recall[0].item()
        paraphrase.similarities["bert-score-f1"] = f1[0].item()

    def bulk_score(self, paraphrases):
        from bert_score import score as bert_score
        import transformers
        import logging

        transformers.tokenization_utils.logger.setLevel(logging.ERROR)
        transformers.configuration_utils.logger.setLevel(logging.ERROR)
        transformers.modeling_utils.logger.setLevel(logging.ERROR)

        first_texts = []
        second_texts = []

        for paraphrase in paraphrases:
            first_texts.append(paraphrase.first_entry.text)
            second_texts.append(paraphrase.second_entry.text)

        precision, recall, f1 = bert_score(first_texts, second_texts, lang="en", rescale_with_baseline=True)

        for prec, rec, f1, p in zip(precision, recall, f1, paraphrases):
            p.similarities["bert-score-precision"] = prec.item()
            p.similarities["bert-score-recall"] = rec.item()
            p.similarities["bert-score-f1"] = max(0, min(1, f1.item()))
