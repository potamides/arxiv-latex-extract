import concurrent.futures
from datetime import datetime
from tqdm import tqdm
import fnmatch
import gzip
import json
import logging
import lzma
import os
import pathlib
import re
from subprocess import CalledProcessError, DEVNULL, run
import tarfile
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Dict, List, Optional, Tuple, Union
import uuid

from . import ARXIV_URL

class ArxivCleaner:
    r""" Class for cleaning raw arxiv data. """

    def __init__(
            self,
            data_dir: pathlib.Path | str,
            work_dir: pathlib.Path | str,
            target_dir: pathlib.Path | str,
            worker_id: Optional[str] = None,
            filter_func = lambda _: True
    ):
        self._data_dir = pathlib.Path(data_dir)
        self._work_dir = pathlib.Path(work_dir)
        self._target_dir = pathlib.Path(target_dir)
        self._worker_id = worker_id if worker_id else str(uuid.uuid4())
        self.filter_func = filter_func

        # make sure dirs exist
        for d in [self._work_dir, self._target_dir]:
            if not d.exists():
                d.mkdir(parents=True)

    def run_parallel(
            self, max_files: int = -1, workers: Optional[int] = None,
            tar_fp_list: Optional[List[str]] = None,
            compress: bool = False,
            verbose: bool = False
    ):
        r""" function to run the cleaning process in parallel. This function
        will iterate over all arxiv projects and clean the tex files. The
        cleaned tex files are then written to a jsonl file.
        @param max_files: maximum number of files to process, defaults to -1
            which means all files are processed. This is useful for testing.
        @param workers: number of workers to use, defaults to None which means
            that all cores are used.
        @param tar_fp_list: list of tars to process. Defaults to None which
            means that all files in data_dir are processed.
        """
        out_file = self._target_dir / (f"arxiv_{self._worker_id}.jsonl" + (".xz" if compress else ""))
        with open(out_file, "wb") as f:
            with concurrent.futures.ProcessPoolExecutor(workers) as executor:
                for record, arxiv_id in executor.map(
                        create_record_single_arg,
                        tqdm(self.arxiv_iterator(max_files=max_files, tar_fp_list=tar_fp_list), disable=not verbose)
                ):
                    if record is None:
                        logging.error(f"failed  to process {arxiv_id}")
                        continue

                    if len(record["text"]) == 0:
                        logging.warning(f"empty text for {arxiv_id}")
                        continue

                    if compress:
                        f.write(lzma.compress((json.dumps(record) + "\n").encode()))
                    else:
                        f.write((json.dumps(record) + "\n").encode())
                    logging.info(f"processed {arxiv_id}")

                executor.shutdown(wait=True)

    def run(self, max_files: int = -1, out_fname: str = "arxiv.jsonl", compress: bool = False, verbose: bool = False):
        r""" function to run the cleaning process. This function will iterate
        over all arxiv projects and clean the tex files. The cleaned tex files
        are then written to a jsonl file.

        @param max_files: maximum number of files to process, defaults to -1
            which means all files are processed. This is useful for testing.
        @param out_fname: name of the output file, defaults to "arxiv.jsonl"
        """
        with open(self._target_dir / (out_fname + (".xz" if compress else "")), "wb") as f:
            for tex_file, yymm, arxiv_id, timestamp in tqdm(self.arxiv_iterator(max_files=max_files), disable=not verbose):
                record, arxiv_id = create_record(
                    tex_file=tex_file,
                    yymm=yymm,
                    arxiv_id=arxiv_id,
                    timestamp=timestamp
                )

                if record is None:
                    logging.error(f"failed to process {arxiv_id}")
                    continue

                if len(record["text"]) == 0:
                    logging.warning(f"empty text for {arxiv_id}")
                    continue

                if compress:
                    f.write(lzma.compress((json.dumps(record) + "\n").encode()))
                else:
                    f.write((json.dumps(record) + "\n").encode())
                logging.info(f"processed {arxiv_id}")

    def arxiv_iterator(
            self, max_files: int = -1, tar_fp_list: Optional[List[str]] = None
    ):
        r""" iterator over arxiv shards. Each shard contains tex projects or
        files that are compressed using gzip. This function will extract the
        tex files and yield them together with yymm, the raw arxiv id and the
        timestamp of the project.

        @param max_files: maximum number of files to process, defaults to -1
            which means all files are processed.
        @param tar_fp_list: optional list of tar files to process, defaults to
            None. In this case all tar files in data_dir are processed.

        @return: iterator over tex files, yymm, arxiv id and timestamp.
        """
        def _tar_fp_iterator():
            for _tar_fp in tar_fp_list or self._data_dir.glob("*.tar"):
                yield _tar_fp

        failed = 0
        processed = 0

        for tar_fp in _tar_fp_iterator():
            logging.info("start processing {tar_fp}")

            with TemporaryDirectory(dir=self._work_dir) as tmpdir:
                with tarfile.open(tar_fp) as tf:
                    tf.extractall(members=tf.getmembers(), path=tmpdir)

                    for proj_dir_or_file in pathlib.Path(tmpdir).rglob("*.gz"):

                        # get arxiv id and month from the filename
                        yymm = proj_dir_or_file.parent.stem
                        arxiv_id = proj_dir_or_file.stem

                        # load the tex source files (we also get the timestamp
                        # here)
                        data = _tex_proj_loader(proj_dir_or_file, self.filter_func)

                        if data is None:
                            failed += 1
                            continue

                        tex_file, timestamp = data
                        processed += 1

                        if processed > max_files > 0:
                            break

                        yield tex_file, yymm, arxiv_id, timestamp

                    else:
                        continue
                    break

        logging.info("Failed loading : {failed}")
        logging.info("done.")


def latexpand(tex_file_path):
    r"""
    Flatten LaTeX file by expanding \include and \input, ... and remove comments
    """
    with NamedTemporaryFile(buffering=0) as tmp:
        cmd = ['latexpand', tex_file_path, "--output", tmp.name]
        run(cmd, stdout=DEVNULL, stderr=DEVNULL, check=True)

        tmp.seek(0)
        return tmp.read().strip()


def latexpand_str(latex):
    with NamedTemporaryFile(buffering=0) as tmp:
        tmp.write(latex)
        return latexpand(tmp.name)


def find_root_file(directory="."):
    first_file = None
    for root, _, filenames in os.walk(directory):
        for filename in fnmatch.filter(filenames, '*.tex'):
            path = os.path.join(root, filename)
            if not first_file:
                first_file = path
            with open(path, 'rb') as file:
                content = file.read()
                if any(pattern in content for pattern in [rb'\documentclass', rb'\documentstyle']):
                    return path
    if first_file:
        return first_file # fallback
    raise FileNotFoundError


def format_arxiv_id(arxiv_id: str) -> str:
    r""" this function brings the raw arxiv-id into a format compliant with the
    specification from arxiv. This is used to create the url to the arxiv
    abstract page.

    - Format prior to March 2007:
        <archive>/YYMMNNN where N is a 3-digit number
    - Format after March 2007: <archive>/YYMM.NNNNN where N is a 5 (or 6)-digit
        number

    References: https://info.arxiv.org/help/arxiv_identifier.html

    @param arxiv_id: raw arxiv id which can be in one of the following formats:
        - <archive><YY><MM><NNN>
        - <YY><MM><NNNNN|NNNNNN>

    @return: formatted arxiv id
    """
    match = re.search(r'^([a-zA-Z-]*)([\d\.]+)$', arxiv_id)

    if match is None:
        raise ValueError(f"Invalid arxiv id: {arxiv_id}")

    if match.group(1) == "":
        return match.group(2)

    return f"{match.group(1)}/{match.group(2)}"


def create_record_single_arg(args):
    r""" convenience function to create a record from a single argument. """
    return create_record(*args)


def create_record(
        tex_file: str,
        yymm: str,
        arxiv_id: str,
        timestamp: float
) -> Tuple[Union[Dict[str, Union[str, Dict[str, str]]], str, None], str]:
    r""" function to create a record from the tex files, yymm, arxiv id and
    timestamp.

    @param tex_file: list of tex file contents as strings
    @param yymm: yymm of the arxiv project
    @param arxiv_id: raw arxiv id
    @param timestamp: timestamp of the arxiv project

    @return: dictionary containing the cleaned tex text and metadata
    """
    if len(tex_file) == 0:
        return {"text": "", "meta": {}}, arxiv_id

    # get the arxiv id in the correct format
    try:
        clean_arxiv_id = format_arxiv_id(arxiv_id)
    except Exception as e:
        logging.warning(f"failed to format arxiv id {arxiv_id}; excpetion={e}")
        clean_arxiv_id = arxiv_id

    if timestamp is not None:
        timestamp = datetime.fromtimestamp(timestamp).isoformat()

    return (
        {
            "text": tex_file,
            "meta": {
                "timestamp": timestamp,
                "yymm": yymm,
                "arxiv_id": clean_arxiv_id,
                "url": f"{ARXIV_URL}{clean_arxiv_id}",
                "source": "arxiv"
            }
        },
        clean_arxiv_id
    )


def matches(directory, filter_func):
    for root, _, filenames in os.walk(directory):
        for filename in fnmatch.filter(filenames, '*.tex'):
            path = os.path.join(root, filename)
            with open(path, 'rb') as file:
                if filter_func(file.read()):
                    return True
    return False


def _tex_proj_loader(
        file_or_dir_path: pathlib.Path, filter_func = lambda _: True
) -> Union[Tuple[str, float], None]:
    r""" function to load the tex files from a tar file or a gzip file. The
    function will return a tuple containing a list of tex files and the
    timestamp of the project.

    @param file_or_dir_path: path to the tar file or the gzip file

    @return: tuple containing a list of tex files and the timestamp of the
        project
    """
    timestamp = file_or_dir_path.lstat().st_mtime

    try:
        with TemporaryDirectory() as tmpdir:
        # if it is a directory, open it as a tarfile
            with tarfile.open(file_or_dir_path, "r") as sub_tf:
                sub_tf.extractall(path=tmpdir)
                try:
                    if matches(tmpdir, filter_func):
                        file_content = latexpand(find_root_file(tmpdir))
                    else:
                        return None
                except (FileNotFoundError, CalledProcessError) as e:
                    logging.error(f"{type(e).__name__}: {file_or_dir_path}")
                    return None

    except tarfile.ReadError:
        # otherwise we try opening it as a gzip file
        try:
            with gzip.open(file_or_dir_path, "rb") as gz:
                file_content = latexpand_str(gz.read())
        except Exception as e:
            # all fails, we skip this file
            logging.error(f"{type(e).__name__}: {file_or_dir_path}")
            return None

    except Exception as e:
        logging.error(f"{type(e).__name__}: {file_or_dir_path}")
        return None

    for idx, encoding in enumerate(encodings:=["utf-8", "latin1"]):
        try:
            return file_content.decode(encoding), timestamp
        except ValueError:
            if idx == len(encodings) -1:
                logging.error(f"DecodeError: {file_or_dir_path}")
