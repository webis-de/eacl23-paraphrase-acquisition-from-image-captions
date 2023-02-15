import json
import os.path

from paraphrase.paraphrase import Paraphrase, ParaphraseEntry
from paraphrase.parser.parser import Parser


class MSCOCOParser(Parser):
    NAME = "mscoco"

    def __init__(self, file_dir):
        super().__init__([os.path.join(file_dir, "annotations/captions_val2017.json"),
                          os.path.join(file_dir, "annotations/captions_train2017.json")])
        self.buffer = []

    def get_next(self):
        if len(self.buffer) == 0:
            line = self.in_file.readline()

            if line == "":
                return None

            data = json.loads(line)

            image_groups = {}
            for annotation in data["annotations"]:
                if annotation["image_id"] not in image_groups:
                    image_groups[annotation["image_id"]] = []

                image_groups[annotation["image_id"]].append(annotation)

            for image_id in image_groups:
                for i in range(len(image_groups[image_id])):
                    for j in range(len(image_groups[image_id])):
                        if i < j:
                            first = image_groups[image_id][i]
                            second = image_groups[image_id][j]
                            self.buffer.append(
                                Paraphrase(str(first["id"]) + "-" + str(second["id"]),
                                           ParaphraseEntry(first["caption"]),
                                           ParaphraseEntry(second["caption"]),
                                           image_id,
                                           similarities={},
                                           is_paraphrase=None)
                            )

        return self.buffer.pop()
