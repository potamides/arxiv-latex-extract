from datetime import datetime
from functools import partial
from os import listdir
from threading import TIMEOUT_MAX
from os.path import join
from shutil import rmtree

import internetarchive as ia

from . import ARCHIVE_DIR, LATEX_DIR

items = ia.search_items('collection:arxiv-bulk')

def _make_filter(timestamp):  # only these files seem to contain source files (opposed to only pdf)
    def _filter(item):
        return "_src_" in item['identifier'] and _to_timestamp(item) >= timestamp

    return _filter

def _to_timestamp(item):
    return datetime.strptime(item['identifier'], "arXiv_src_%y%m_%f")

def _item_download(identifier, index, verbose=False, pattern="*.tar", timeout=TIMEOUT_MAX):
    item = ia.get_item(identifier, request_kwargs=dict(timeout=timeout))
    item.download(destdir=ARCHIVE_DIR, verbose=verbose, glob_pattern=pattern, item_index=index, timeout=timeout)
    return join(ARCHIVE_DIR, identifier)

def download(lazy=False, cutoff=datetime.fromtimestamp(0), **kwargs):
    archives = sorted(filter(_make_filter(cutoff), items), key=_to_timestamp, reverse=True) # sort by date
    for idx, item in enumerate(archives):
        identifier = item['identifier']
        if not any(extracted.startswith(identifier) for extracted in listdir(LATEX_DIR)):
            if lazy:
                yield partial(_item_download, identifier, idx, **kwargs)
            else:
                _item_download(identifier, idx, **kwargs)

def delete(path):
    if path.startswith(ARCHIVE_DIR):
        rmtree(path)
