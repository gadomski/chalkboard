import os.path
import sys
import subprocess
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

infile = sys.argv[1]
account = sys.argv[2]
container = sys.argv[3]
sas = sys.argv[4]


def run(args):
    print(f"Running {args}")
    subprocess.run(args)


with open(infile) as file:
    with TemporaryDirectory() as temporary_directory:
        for line in file:
            source = line.rstrip()
            file_name = os.path.basename(source)
            temporary_path = os.path.join(temporary_directory, file_name)
            run(["wget", source, "-O", temporary_path])
            path = urlparse(source).path
            destination = (
                f"https://{account}.blob.core.windows.net/{container}{path}?{sas}"
            )
            run(["azcopy", "cp", temporary_path, destination])
