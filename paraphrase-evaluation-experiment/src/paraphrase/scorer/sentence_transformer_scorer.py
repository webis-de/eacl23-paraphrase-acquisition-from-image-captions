from paraphrase.scorer.scorer import Scorer


class SentenceTransformerScorer(Scorer):
    def __init__(self, *args):
        from sentence_transformers.SentenceTransformer import SentenceTransformer
        self.sentence_transformer = SentenceTransformer("paraphrase-mpnet-base-v2")

    def score(self, paraphrase, first_tokens, second_tokens):
        from sentence_transformers.util import cos_sim
        embeddings = self.sentence_transformer.encode([paraphrase.first_entry.text, paraphrase.second_entry.text],
                                                      convert_to_tensor=True)

        paraphrase.similarities["sentence-transformer-cos-sim"] = cos_sim(embeddings[0], embeddings[1])[0][0].item()
        paraphrase.similarities["sentence-transformer-cos-sim"] = min(1, paraphrase.similarities["sentence-transformer-cos-sim"])
        paraphrase.similarities["sentence-transformer-cos-sim"] = max(0, paraphrase.similarities["sentence-transformer-cos-sim"])
