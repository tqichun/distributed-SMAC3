import os
from pathlib import Path

from hdfs.client import Client


class FileSystem():
    def listdir(self, parent, **kwargs):
        raise NotImplementedError

    def join(self, path, *paths):
        os.path.join(path, *paths)

    def read_txt(self, path):
        raise NotImplementedError

    def write_txt(self, path, txt,append=False):
        raise NotImplementedError

    def mkdir(self, path, **kwargs):
        raise NotImplementedError


class HDFS(FileSystem):
    def __init__(self, url):
        self.client = Client(url)

    def listdir(self, parent, **kwargs):
        return self.client.list(parent, kwargs.get('status', False))

    def read_txt(self, path):
        with self.client.read(path) as f:
            b: bytes = f.read()
            txt = b.decode(encoding='utf-8')
            return txt

    def write_txt(self, path, txt,append=False):
        if append:
            self.client.write(path, txt, overwrite=False, append=True)
        else:
            self.client.write(path, txt, overwrite=True, append=False)

    def mkdir(self, path, **kwargs):
        self.client.makedirs(path)


class LocalFS(FileSystem):
    def listdir(self, parent, **kwargs):
        return os.listdir(parent)

    def read_txt(self, path):
        return Path(path).read_text()

    def write_txt(self, path, txt,append=False):
        if append:
            mode='a+'
        else:
            mode='w'
        with open(path,mode) as f:
            f.write(txt)

    def mkdir(self, path, **kwargs):
        Path(path).mkdir(exist_ok=kwargs.get('exist_ok', True), parents=kwargs.get('parents', True))
