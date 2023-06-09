#!/usr/bin/env python
from datetime import datetime
from functools import partial
from multiprocessing import Pool
from os import devnull, sched_getaffinity
from os.path import basename
import sys
from tempfile import TemporaryDirectory
from typing import Callable

from tqdm import tqdm

from ale import LATEX_DIR
from ale.arxiv import delete, download
from ale.cleaner import ArxivCleaner

def clean(archive, output, filter_func=lambda _: True, verbose=False):
    # create temporary work directory
    with TemporaryDirectory() as work_dir:
        arxiv_cleaner = ArxivCleaner(
            data_dir=archive,
            work_dir=work_dir,
            target_dir=LATEX_DIR,
            filter_func=filter_func
        )

        arxiv_cleaner.run(out_fname=output, verbose=verbose)

def process(archive, **kwargs):
    if isinstance(archive, Callable): # handle lazy downloading
        archive = archive()

    output = f"{basename(archive)}.jsonl"
    clean(archive, output, **kwargs)
    delete(archive)

if __name__ == "__main__":
    def filter_func(tex): return b"tikzpicture" in tex # only process projects which contain tikz
    cutoff = datetime(2010, 1, 1) # do not process papers older than 2010

    # Parallelize to make things faster
    with Pool(num_workers:=len(sched_getaffinity(0))) as p:
        tasks = list(download(lazy=True, cutoff=cutoff))

        print(f"Parallel processing on {num_workers} workers.")
        for _ in tqdm(p.imap_unordered(partial(process, filter_func=filter_func), tasks), total=len(tasks)):
            pass
