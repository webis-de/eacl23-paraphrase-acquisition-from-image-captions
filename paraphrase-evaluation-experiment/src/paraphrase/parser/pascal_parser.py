import os.path

from paraphrase.paraphrase import Paraphrase, ParaphraseEntry
from paraphrase.parser.parser import Parser


class PascalParser(Parser):
    NAME = "pascal"

    def __init__(self, file_dir):
        paths = []
        for root, dirs, files in os.walk(os.path.join(file_dir, "sentence")):
            for file in files:
                if file.endswith("txt"):
                    paths.append(os.path.join(root, file))

        super().__init__(paths)

        self.last_line = None
        self.buffer = []

    def get_next(self):
        if len(self.buffer) == 0:
            filename = self.in_file.filename()
            sentences = []

            if self.last_line is not None:
                sentences.append(self.last_line)

            while True:
                line = self.in_file.readline().replace("\n", "")

                if line == "":
                    break

                self.last_line = line

                if filename is None:
                    filename = self.in_file.filename()

                if filename != self.in_file.filename():
                    break

                sentences.append(line)

            for i in range(len(sentences)):
                for j in range(len(sentences)):
                    if i < j:
                        self.buffer.append(Paraphrase(os.path.basename(filename) + "-" + str(i) + "-" + str(j),
                                                      ParaphraseEntry(sentences[i]),
                                                      ParaphraseEntry(sentences[j]),
                                                      similarities={},
                                                      is_paraphrase=None))

        if len(self.buffer) == 0:
            return None

        return self.buffer.pop()
