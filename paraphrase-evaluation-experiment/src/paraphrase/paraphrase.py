class Paraphrase:

    def __init__(self, id, first_entry, second_entry, cluster_id=None, image=None, similarities=None,
                 is_paraphrase=None):
        self.id = id
        self.cluster_id = cluster_id
        self.image = image
        self.first_entry = first_entry
        self.second_entry = second_entry
        self.similarities = similarities

        if self.similarities is None:
            self.similarities = {}

        self.is_paraphrase = is_paraphrase

    def to_dict(self):
        return {"id": self.id,
                "cluster-id": self.cluster_id,
                "image": self.image,
                "first": {"caption-text": self.first_entry.text,
                          "caption-type": self.first_entry.origin,
                          "page": self.first_entry.source},
                "second": {"caption-text": self.second_entry.text,
                           "caption-type": self.second_entry.origin,
                           "page": self.second_entry.source},
                "similarities": self.similarities,
                "is-paraphrase": self.is_paraphrase
                }


class ParaphraseEntry:
    def __init__(self, text, origin=None, source_page=None):
        self.text = text
        self.origin = origin
        self.source = source_page
