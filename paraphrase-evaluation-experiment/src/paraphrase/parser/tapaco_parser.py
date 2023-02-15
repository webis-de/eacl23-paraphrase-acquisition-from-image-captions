import os

from paraphrase.paraphrase import Paraphrase, ParaphraseEntry
from paraphrase.parser.parser import Parser


class TaPaCoParser(Parser):
    NAME = "tapaco"

    def __init__(self, file_dir):
        super().__init__([os.path.join(file_dir, "en.txt")])
        self.buffer = []
        self.last_line = None
        self.c_id = "1"

    def get_next(self):
        if len(self.buffer) == 0:
            tmp = []
            while True:
                if self.last_line is not None:
                    line = self.last_line
                    self.last_line = None
                else:
                    line = self.in_file.readline()

                if line == "":
                    return None

                values = line.replace("\n", "").split("\t")
                if self.c_id is None:
                    self.c_id = values[0]

                if values[0] != self.c_id:
                    self.last_line = line
                    self.c_id = None
                    break

                tmp.append(values)

            for i in range(len(tmp)):
                for j in range(len(tmp)):
                    if i < j:
                        self.buffer.append(Paraphrase(tmp[i][1] + "-" + tmp[j][1],
                                                      ParaphraseEntry(tmp[i][2]),
                                                      ParaphraseEntry(tmp[j][2]),
                                                      is_paraphrase=True))

        return self.buffer.pop()




