import json
import os.path

import click
from dotenv import load_dotenv
from nltk.stem import porter
from rouge_score import tokenize

from paraphrase.parser.parser import dynamic_load_parsers
from paraphrase.scorer.bleu_scorer import BleuScorer
from paraphrase.scorer.rouge_scorer import RougeScorer
from paraphrase.scorer.word_mover_distance_scorer import WordMoverDistanceScorer


@click.command()
@click.option("-d", "--dataset",
              type=click.Choice(["wikipedia-ipc", "paws", "msrpc", "mscoco", "para-nmt", "pascal",
                                 "tapaco", "ppdb", "flickr8k"]),
              required=True, default="wikipedia-ipc")
@click.option("-o", "--out", type=click.Path(exists=False, file_okay=False), required=False)
@click.option("-s", "--scorer", type=click.Choice(["bleu", "rouge", "sumo", "nist", "wmd"]), required=True,
              default=["bleu", "rouge", "wmd"], multiple=True)
def main(dataset, out, scorer):
    conf_path = os.path.dirname(__file__)
    conf_path = os.path.join(conf_path, "../../conf/corpora/", dataset + ".env")

    parsers = dynamic_load_parsers()
    load_dotenv(dotenv_path=conf_path)
    parser_instance = parsers[dataset](os.getenv("HOST_INPUT_DIR"))

    if out is None:
        out = os.path.dirname(__file__)
        out = os.path.join(out, "../../data/out")

    if not os.path.exists(out):
        os.mkdir(out)

    scorers = {"bleu": BleuScorer(4), "rouge": RougeScorer(),
               "wmd": WordMoverDistanceScorer()}

    scorer_instances = [scorers[x] for x in scorer]

    paraphrase = parser_instance.get_next()
    stemmer = porter.PorterStemmer()

    with open(os.path.join(out, dataset + "@syntax-scores.jsonl"), "w") as out_file:
        while paraphrase is not None:
            first_tokens = tokenize.tokenize(paraphrase.first_entry.text, stemmer)
            second_tokens = tokenize.tokenize(paraphrase.second_entry.text, stemmer)

            for scorer_instance in scorer_instances:
                scorer_instance.score(paraphrase,
                                      first_tokens,
                                      second_tokens)

            out_dict = paraphrase.to_dict()

            json.dump(out_dict, out_file)
            out_file.write("\n")

            paraphrase = parser_instance.get_next()


if __name__ == '__main__':
    main()
