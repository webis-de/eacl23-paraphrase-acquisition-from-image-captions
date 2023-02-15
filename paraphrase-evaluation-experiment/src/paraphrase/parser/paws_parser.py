import os.path

from paraphrase.paraphrase import Paraphrase, ParaphraseEntry
from paraphrase.parser.parser import Parser


class PAWSParser(Parser):
    NAME = "paws"

    def __init__(self, file_dir):
        files = [os.path.join(file_dir, "final/dev.tsv"),
                 os.path.join(file_dir, "final/test.tsv"),
                 os.path.join(file_dir, "final/train.tsv")]
        super().__init__(files)

    def get_next(self):
        line = self.in_file.readline()

        if line == "":
            return None

        if line.startswith("id"):
            return self.get_next()

        values = line.replace("\n", "").split("\t")

        paraphrase = Paraphrase(
            values[0].strip(),
            ParaphraseEntry(values[1]),
            ParaphraseEntry(values[2]),
            is_paraphrase=("1" == values[3].strip())
        )

        return paraphrase


if __name__ == '__main__':
    parser = PAWSParser("data/paws")

    example = parser.get_next()

    while example is not None:
        print(example.__dict__)
        example = parser.get_next()
