from os import makedirs

ARCHIVE_DIR = "archives"
LATEX_DIR = "extracted"
ARXIV_URL = "https://arxiv.org/abs/"

for path in [ARCHIVE_DIR, LATEX_DIR]:
    makedirs(path, exist_ok=True)
