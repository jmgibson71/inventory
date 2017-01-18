import hashlib
from pathlib import Path
import logging
import os
import time


class SampleHash:
    def __init__(self, file, hash_type=hashlib.md5):
        self.logger = logging.getLogger()
        try:
            self.hash_t = hash_type
            self.file_name = Path(file)
            self.file_size = self.file_name.stat().st_size
            self.bytes_to_sample = 3*1000**2
            self.megabyte = 1000**2
            self.chunk_size = int(self.bytes_to_sample / 10)
            self.mini_chunks = int(self.chunk_size / 4000)
            self.end = int(3)
        except FileNotFoundError as e:
            raise FileNotFoundError(e)

    def hash(self, full_only=False):
        if full_only:
            return self._full_hash()

        if int((self.file_size / self.megabyte)) > self.end:
            return self._quick_hash()
        else:
            return self._full_hash()

    def _quick_hash(self):
        hash_l = self.get_hasher()
        with open(str(self.file_name), "rb") as f:
            chunk_list = self.chunk_list()
            for p in chunk_list:
                f.seek(p)
                for ch in range(self.mini_chunks):
                    hash_l.update(f.read(4000))
        return hash_l.hexdigest()

    def _quick_hash_front_read(self):
        hash_l = self.get_hasher()
        end = self.bytes_to_sample
        with open(str(self.file_name), "rb") as f:
            while f.tell() < end:
                hash_l.update(f.read(4000))
        return hash_l.hexdigest()

    def _full_hash(self):
        hash_l = self.get_hasher()
        for p in self.lazy_read(open(str(self.file_name), "rb")):
            hash_l.update(p)
        return hash_l.hexdigest()

    def lazy_read(self, file_object, chunk_size=4000):
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    def chunk_list(self):
        fs = self.file_size
        positions = []
        for i in range(0, 100, 10):
            positions.append(int(fs*(i/100)))
        return positions

    def get_hasher(self):
        if self.hash_t is hashlib.md5:
            return hashlib.md5()
        if self.hash_t is hashlib.sha256:
            return hashlib.sha256()
        if self.hash_t is hashlib.sha1:
            return hashlib.sha1()
        if self.hash_t is hashlib.sha224:
            return hashlib.sha224()
        if self.hash_t is hashlib.sha384:
            return hashlib.sha384()
        if self.hash_t is hashlib.sha512:
            return hashlib.sha512()



if __name__ == "__main__":
    for root, dirs, files in os.walk("S:\\Staging\\Order"):
        for f in files:
            sh = SampleHash(os.path.join(root, f), hashlib.sha256)
            pos = sh.chunk_list()
            start = time.time()
            print("Hashing; {}".format(sh.file_name))
            print(sh.hash(True))
            end = time.time()
            print("Hashing took: {}s".format(end - start))