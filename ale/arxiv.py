from abc import ABC, abstractmethod
from datetime import datetime
from functools import cached_property, partial
from itertools import chain
from os import getenv, listdir, makedirs
from os.path import join
from shutil import rmtree
from threading import TIMEOUT_MAX
from typing import List

import internetarchive as ia

from . import ARCHIVE_DIR, LATEX_DIR

try:
    import boto3
    has_boto3 = True
except ImportError:
    has_boto3 = False


class BaseDownloader(ABC):
    def _make_filter(self, timestamp, exclude=list()):  # only these files seem to contain source files (opposed to only pdf)
        def _filter(item):
            is_src = lambda: "_src_" in item
            is_excluded = lambda: any(ex in item for ex in exclude)
            too_old = lambda: self._to_timestamp(item) < timestamp
            already_processed = lambda: any(extracted.startswith(item) for extracted in listdir(LATEX_DIR))

            return is_src() and not is_excluded() and not too_old() and not already_processed()

        return _filter

    def _to_timestamp(self, item):
        return datetime.strptime(item, "arXiv_src_%y%m_%f")

    @property
    @abstractmethod
    def items(self) -> List[str]:
        """Returns all items that in the sources."""

    @abstractmethod
    def _item_download(self, identifier, index, verbose=False, **kwargs):
        """Source specific implementation of item downloading."""

    def download(self, lazy=False, cutoff=datetime.fromtimestamp(0), exclude=list(), **dl_kwargs):
        archives = sorted(filter(self._make_filter(cutoff, exclude), self.items), key=self._to_timestamp, reverse=True) # sort by date
        for idx, item in enumerate(archives):
            if lazy:
                yield partial(self._item_download, item, idx, **dl_kwargs)
            else:
                self._item_download(item, idx, **dl_kwargs)


class ArchiveDownloader(BaseDownloader):
    @cached_property
    def items(self):
        return [item['identifier'] for item in ia.search_items('collection:arxiv-bulk')]

    def _item_download(self, identifier, index, verbose=False, pattern="*.tar", timeout=TIMEOUT_MAX, **_):
        item = ia.get_item(identifier, request_kwargs=dict(timeout=timeout))
        item.download(destdir=ARCHIVE_DIR, verbose=verbose, glob_pattern=pattern, item_index=index, timeout=timeout)
        return join(ARCHIVE_DIR, identifier)


class S3Downloader(BaseDownloader):
    access_key = getenv("AWS_ACCESS_KEY")
    secret_key = getenv("AWS_SECRET_KEY")

    @property
    def has_aws_access(self):
        return has_boto3 and self.access_key and self.secret_key

    @property
    def _s3resource(self):
        return boto3.resource(
            "s3",  # the AWS resource we want to use
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1",  # same region arxiv bucket is in
        )

    @cached_property
    def items(self):
        if self.has_aws_access:
            # Create a reusable Paginator
            paginator = self._s3resource.meta.client.get_paginator("list_objects_v2")  # type: ignore

            # Create a PageIterator from the Paginator
            iterator = paginator.paginate(
                Bucket="arxiv", RequestPayer="requester", Prefix="src/"

            )
            return list(
                chain.from_iterable(
                    [
                        file["Key"].removeprefix("src/").removesuffix(".tar")
                        for file in page["Contents"]
                        if file["Key"].endswith(".tar")
                    ]
                    for page in iterator
                )
            )
        else:
            return list()

    def _item_download(self, identifier, index, verbose=False, **_):
        src_file, target_dir = f"{identifier}.tar", join(ARCHIVE_DIR, identifier)

        if verbose:
            print(f"Downloading {identifier} ({index})...")

        makedirs(target_dir, exist_ok=True)
        self._s3resource.meta.client.download_file( # type: ignore
            Bucket='arxiv',
            Key=join("src", src_file),
            Filename=join(target_dir, src_file),
            ExtraArgs={'RequestPayer': 'requester'}
       )

        if verbose:
            print(f"Finished downloading {identifier} ({index}).")

        return join(ARCHIVE_DIR, identifier)


def download(*args, **kwargs):
    arxiv, s3 = ArchiveDownloader(), S3Downloader()

    yield from arxiv.download(*args, **kwargs)
    yield from s3.download(*args, exclude=arxiv.items, **kwargs)

def delete(path):
    if path.startswith(ARCHIVE_DIR):
        rmtree(path)
