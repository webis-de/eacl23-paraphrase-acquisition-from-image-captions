import os

from paraphrase.paraphrase import Paraphrase, ParaphraseEntry
from paraphrase.parser.parser import Parser


class PPDBParser(Parser):
    NAME = "ppdb"

    def __init__(self, file_dir):
        super().__init__([os.path.join(file_dir, "ppdb-2.0-xxxl-all")])
        self.accepted = {"Equivalence", "ForwardEntailment", "ReverseEntailment", "OtherRelated"}

    def get_next(self):
        while True:
            line = self.in_file.readline()

            if line == "":
                return None

            values = line.replace("\n", "").split(" ||| ")
            scores = values[3].split(" ")

            if len(values[1].split(" ")) >= 4 and len(values[2].split(" ")) >= 4:
                if not ("[" in values[1] or "]" in values[1] or "[" in values[2] or "]" in values[2]):
                    score_dict = {}
                    for score in scores:
                        score_split = score.split("=")
                        score_dict[score_split[0]] = score_split[1]

                    paraphrase = Paraphrase(self.in_file.lineno(),
                                            ParaphraseEntry(values[1]),
                                            ParaphraseEntry(values[2]),
                                            similarities=score_dict,
                                            is_paraphrase=True)

                    return paraphrase
