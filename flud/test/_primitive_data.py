from binascii import crc32
from dataclasses import dataclass
from io import StringIO
import os
import random
import shutil

from flud.FludCrypto import generateRandom, hashfile, hashstring
from flud.fencode import fencode


METADATA_BLOCK = fencode((1, 20, 40, "adfdsfdffffffddddddddddddddd"))
METADATA_BLOCK_BYTES = METADATA_BLOCK.encode("utf-8")
FAKE_MKEY_OFFSET = 111111


@dataclass
class PrimitiveCase:
    size_name: str
    filekey: str
    path: str


@dataclass
class PrimitiveFailureCase:
    size_name: str
    filekey: str
    path: str
    bad_path: str


def metadata_key(path):
    return crc32(path.encode("utf-8")) & 0xFFFFFFFF


def metadata_for_key(mkey):
    return (mkey, StringIO(METADATA_BLOCK))


def metadata_for_path(path):
    return metadata_for_key(metadata_key(path))


def create_case_file(tmp_path, min_size, size_name):
    seed_path = tmp_path / f"{size_name}.bin"
    chunk = generateRandom(max(1, min_size // 50))
    with seed_path.open("wb") as handle:
        for _ in range(0, 51 + random.randrange(50)):
            handle.write(chunk)
    filekey = fencode(int(hashfile(str(seed_path)), 16))
    good_path = tmp_path / filekey
    shutil.move(str(seed_path), str(good_path))
    return PrimitiveCase(size_name=size_name, filekey=filekey, path=str(good_path))


def create_failure_case_file(tmp_path, min_size, size_name):
    case = create_case_file(tmp_path, min_size, size_name)
    bad_path = tmp_path / ("bad" + case.filekey[3:])
    shutil.copy(case.path, bad_path)
    return PrimitiveFailureCase(
        size_name=case.size_name,
        filekey=case.filekey,
        path=case.path,
        bad_path=str(bad_path),
    )


def find_retrieved_payload(saved_paths, filekey):
    return next(path for path in saved_paths if path.endswith(filekey))


def find_retrieved_metadata(saved_paths, filekey, mkey):
    expected = f"{filekey}.{mkey}.meta"
    return next(path for path in saved_paths if path.endswith(expected))


def verify_payload_matches(source_path, retrieved_path):
    with open(source_path, "rb") as source, open(retrieved_path, "rb") as retrieved:
        return source.read() == retrieved.read()


def verify_metadata_matches(metadata_path):
    with open(metadata_path, "rb") as metadata_file:
        return metadata_file.read() == METADATA_BLOCK_BYTES


def sample_verify_range(path, length=20):
    fd = os.open(path, os.O_RDONLY)
    try:
        fsize = os.fstat(fd).st_size
        offset = random.randrange(fsize - length)
        os.lseek(fd, offset, 0)
        data = os.read(fd, length)
    finally:
        os.close(fd)
    return offset, length, hashstring(data)
