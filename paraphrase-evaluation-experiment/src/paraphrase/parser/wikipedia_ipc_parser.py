import json
import os

from paraphrase.paraphrase import Paraphrase, ParaphraseEntry
from paraphrase.parser.parser import Parser


class WikipediaIPCParser(Parser):
    NAME = "wikipedia-ipc"

    def __init__(self, file_dir):
        super().__init__(os.path.join(file_dir, "wikipedia-ipc-silver.jsonl"))

    def get_next(self):
        line = self.in_file.readline()
        if line == "":
            return None

        example = json.loads(line)

        first_entry = ParaphraseEntry(example["first"]["caption-text"],
                                      example["first"]["caption-type"],
                                      example["first"]["page"])

        second_entry = ParaphraseEntry(example["second"]["caption-text"],
                                       example["second"]["caption-type"],
                                       example["second"]["page"])

        paraphrase = Paraphrase(example["id"],
                                first_entry,
                                second_entry,
                                example["cluster-id"],
                                example["image"],
                                example["similarities"],
                                example["is_paraphrase"] if "is_paraphrase" in example else None)

        return paraphrase


# class WikipediaIPCAnnotatedParser(WikipediaIPCParser):
#     def __init__(self, file_dir):
#         Parser.__init__(self, [os.path.join(file_dir, "wikipedia-paraphrases-sentences-annotated.jsonl")])
