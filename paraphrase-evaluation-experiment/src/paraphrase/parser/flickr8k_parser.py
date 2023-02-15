import os.path

from paraphrase.paraphrase import Paraphrase, ParaphraseEntry
from paraphrase.parser.parser import Parser


class Flickr8kParser(Parser):
    NAME = "flickr8k"

    def __init__(self, file_dir):
        super().__init__(os.path.join(file_dir, "flickr8k-captions.txt"))
        self.buffer = []
        self.last_line = None

    def get_next(self):
        if len(self.buffer) == 0:
            if self.last_line == "":
                return None

            if self.last_line is not None:
                line = self.last_line
            else:
                line = self.in_file.readline()

            captions = []
            while True:
                comp = line.replace("\n", "").split("\t")

                image_id = comp[0].split("#")[0]
                last_image_id = None
                if self.last_line is not None:
                    last_comp = self.last_line.replace("\n", "").split("\t")
                    last_image_id = last_comp[0].split("#")[0]

                if image_id != last_image_id and last_image_id is not None:
                    self.last_line = line
                    break

                captions.append(comp[1])

                self.last_line = line
                line = self.in_file.readline()

            for i in range(len(captions)):
                for j in range(len(captions)):
                    if i < j:
                        self.buffer.append(
                            Paraphrase(image_id,
                                       ParaphraseEntry(captions[i]),
                                       ParaphraseEntry(captions[j]),
                                       image=image_id,
                                       similarities={},
                                       is_paraphrase=None)
                        )

        if len(self.buffer) > 0:
            return self.buffer.pop()
