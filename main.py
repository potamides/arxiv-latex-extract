#!/usr/bin/env python
from datetime import datetime
from functools import partial
from multiprocessing import Pool
from os import sched_getaffinity
from os.path import basename
from shutil import copy
from tempfile import TemporaryDirectory
from typing import Callable

from tqdm import tqdm

from ale import LATEX_DIR
from ale.arxiv import delete, download
from ale.cleaner import ArxivCleaner

def clean(archive, output, target_dir=LATEX_DIR, filter_func=lambda _: True, verbose=False):
    # create temporary work directory
    with TemporaryDirectory() as work_dir:
        arxiv_cleaner = ArxivCleaner(
            data_dir=archive,
            work_dir=work_dir,
            target_dir=target_dir,
            filter_func=filter_func
        )

        return arxiv_cleaner.run(out_fname=output, verbose=verbose)

def process(archive, **kwargs):
    if isinstance(archive, Callable): # handle lazy downloading
        archive = archive()

    output = f"{basename(archive)}.jsonl"
    path = clean(archive, output, **kwargs)
    delete(archive)
    return path

if __name__ == "__main__":
    def filter_func(tex): return b"tikzpicture" in tex # only process projects which contain tikz
    cutoff = datetime(2010, 1, 1) # do not process papers older than 2010

    # Parallelize to make things faster
    with Pool(num_workers:=len(sched_getaffinity(0))) as p:
        print(f"Parallel processing on {num_workers} workers.")

        # save results in a tmpdir first and copy them into LATEX_DIR only when
        # processing is finished. This avoids partial files in case of errors
        with TemporaryDirectory() as target_dir:
            tasks = list(download(lazy=True, cutoff=cutoff))
            kwargs = dict(filter_func=filter_func, target_dir=target_dir)

            for path in tqdm(p.imap_unordered(partial(process, **kwargs), tasks), total=len(tasks)):
                copy(path, LATEX_DIR)
