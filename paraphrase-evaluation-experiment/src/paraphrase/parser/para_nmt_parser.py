import os

from paraphrase.paraphrase import Paraphrase, ParaphraseEntry
from paraphrase.parser.parser import Parser


class ParaNMTParser(Parser):
    NAME = "para-nmt"

    def __init__(self, file_dir):
        super().__init__(os.path.join(file_dir, "para-nmt-5m-processed.txt"))

    def get_next(self):
        line = self.in_file.readline()

        if line == "":
            self.in_file.close()
            return None

        values = line.replace("\n", "").split("\t")

        return Paraphrase(self.in_file.lineno(),
                          ParaphraseEntry(values[0]),
                          ParaphraseEntry(values[1]),
                          is_paraphrase=True)
