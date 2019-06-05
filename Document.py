from DBRecord import DBRecord

class Document(DBRecord):
    def __init__(self, name, url, content_type, bid_id, doc_hash=None):
        self.name = name
        self.url = url
        self.content_type = content_type
        self.bid_id = bid_id
        self.doc_hash = None
        self.ocr = None
        self.language  = None

