# ALE: arXiv L<sup>A</sup>T<sub>E</sub>X Extract
ALE is a tool for bulk extracting L<sup>A</sup>T<sub>E</sub>X sources from
[arXiv.org](https://arxiv.org) by processing [arXiv Bulk
Data](https://info.arxiv.org/help/bulk_data_s3.html). Unlike other tools that
exclusively rely on [Amazon S3](https://aws.amazon.com/s3) for downloading, ALE
primarily utilizes the mirror on
[archive.org](https://archive.org/details/arxiv-bulk), which is a free
alternative but may be out-of-date. If optionally
[boto3](https://github.com/boto/boto3) is then also installed and the
environment variables `AWS_ACCESS_KEY` and `AWS_SECRET_KEY` point to valid [AWS
credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/security-creds.html),
missing buckets are retrieved from Amazon S3.

## Installation
Clone the repository and install all requirements.
```sh
git clone https://github.com/potamides/arxiv-latex-extract.git
cd arxiv-latex-extract
pip install -r requirements.txt
```
In addition, this project needs
[`latexpand`](https://gitlab.com/latexpand/latexpand) to flatten
L<sup>A</sup>T<sub>E</sub>X files, so make sure it is installed and on your
`PATH`.

## Usage
To launch the script execute [`main.py`](./main.py):
```sh
python main.py
```
It will display a progress bar and extracted files will be saved in
`extracted/`. Archive files are downloaded to `archives/` as needed and deleted
right after. By default, to keep the number of retrieved files manageable, this
script does only process papers released after January 1st 2010 which contain
the phrase `tikzpicture`. To change this behavior adapt the
[modulino](https://rosettacode.org/wiki/Modulinos) in [`main.py`](./main.py) to
your liking.

## Limitations
While this project worked wonderfully for my task, it is still a messy script
that was hacked together in a short amount of time. Use at your own risk!

## Acknowledgments
The code for cleaning up L<sup>A</sup>T<sub>E</sub>X files is largely based on
the [arXiv processing
code](https://github.com/togethercomputer/RedPajama-Data/tree/main/data_prep/arxiv)
of [RedPajama-Data](https://github.com/togethercomputer/RedPajama-Data).
