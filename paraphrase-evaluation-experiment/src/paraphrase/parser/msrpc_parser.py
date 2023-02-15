import os

from paraphrase.paraphrase import Paraphrase, ParaphraseEntry
from paraphrase.parser.parser import Parser


class MSRPCParser(Parser):
    NAME = "msrpc"

    def __init__(self, file_dir):
        super(MSRPCParser, self).__init__(
            [os.path.join(file_dir, x) for x in os.listdir(file_dir) if x.endswith(".tsv")],
            encoding="utf-8-sig")

    def get_next(self):
        line = self.in_file.readline()

        if line == "":
            self.in_file.close()
            return None

        if line.startswith("Quality"):
            return self.get_next()

        values = line.replace("\n", "").split("\t")

        paraphrase = Paraphrase(values[1] + "-" + values[2],
                                ParaphraseEntry(values[3]),
                                ParaphraseEntry(values[4]),
                                is_paraphrase=("1" == values[0].strip())
                                )

        return paraphrase
