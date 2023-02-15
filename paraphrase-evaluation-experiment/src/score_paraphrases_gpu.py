import time

import click
from dotenv import load_dotenv

from paraphrase.parser.parser import dynamic_load_parsers
from paraphrase.parser.wikipedia_ipc_parser import *
from paraphrase.scorer.bert_scorer import BertScorer
from paraphrase.scorer.bulk_scorer import BulkScorer
from paraphrase.scorer.scorer import Scorer
from paraphrase.scorer.sentence_transformer_scorer import SentenceTransformerScorer
from paraphrase.scorer.word_mover_distance_scorer import WordMoverDistanceScorer


@click.command()
@click.option("-d", "--dataset", type=click.Choice(
    ["flickr8k", "wikipedia-ipc", "paws", "msrpc", "mscoco", "pascal", "para-nmt", "ppdb", "tapaco"]),
              required=True, default="wikipedia-ipc")
@click.option("-o", "--out", type=click.Path(exists=False), required=True, default="/src/data/score.jsonl")
@click.option("-s", "--scorer", type=click.Choice(
    ["bert-score", "sentence-transformer", "word-mover-distance"]), required=True, default="bert-score")
def main(dataset, out, scorer):
    parsers = dynamic_load_parsers()
    scorers = {"bert-score": BertScorer,
               "sentence-transformer": SentenceTransformerScorer,
               "word-mover-distance": WordMoverDistanceScorer}

    conf_path = os.path.dirname(__file__)
    conf_path = os.path.join(conf_path, "../../conf/corpora/", dataset + ".env")
    load_dotenv(dotenv_path=conf_path)
    parser_instance = parsers[dataset](os.getenv("CONTAINER_INPUT_DIR"))
    scorer_instance = scorers[scorer](os.getenv("CONTAINER_INPUT_DIR"))
    processed = 0

    if isinstance(scorer_instance, BulkScorer):
        paraphrases = parser_instance.get_all()
        scorer_instance.bulk_score(paraphrases)
    elif isinstance(scorer_instance, Scorer):
        with open(out, "w") as out_file:
            paraphrase = parser_instance.get_next()
            start = time.time()

            while paraphrase is not None:
                scorer_instance.score(paraphrase, None, None)

                out_dict = paraphrase.to_dict()

                json.dump(out_dict, out_file)
                out_file.write("\n")

                processed += 1

                if time.time() - start > 5:
                    print("NUM PROCESSED: " + str(processed))
                    start = time.time()

                paraphrase = parser_instance.get_next()


if __name__ == '__main__':
    main()
