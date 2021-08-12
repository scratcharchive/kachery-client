class KacheryUri:
    algorithm: str = ''
    hash: str = ''
    filename: str = ''

    def __init__(self, *, algorithm: str, hash: str, filename: str):
        assert algorithm in ['SHA1', 'sha1'], "Unrecognized algorithm."
        # TODO: Do we have criteria for hashes?
        self.algorithm = algorithm
        self.hash = hash
        self.filename = filename

    def emit_uri(self) -> str:
        return f"{self.algorithm}://{self.hash}/{self.filename}"

    def __str__(self):
        return self.emit_uri()
    
    def __repr__(self):
        return f"""
algorithm:\t{self.algorithm}
hash:\t\t{self.hash}
filename:\t{self.filename}"""

def _build_uri(*, uri_object: KacheryUri) -> str:
    return uri_object.emit_uri()

def _parse_uri(*, uri: str) -> KacheryUri:
    # Expected URI format:
    # [ALGORITHM]://[HASH]/[FILENAME]
    parsed = uri.split('/')
    if (len(parsed) < 3):
        raise Exception(f"Cannot parse malformed URI {uri}.")
    
    # remove the trailing : from the algorithm field
    parsed[0] = parsed[0].rstrip(':')

    # handle case where the filename is not set
    if len(parsed) == 3:
        parsed.append('')

    return KacheryUri(
        algorithm=parsed[0].lower(),
        hash=parsed[2],
        filename=parsed[3]
    )
