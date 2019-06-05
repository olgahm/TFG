class DBRecord:
    def to_dict(self):
        return vars(self)