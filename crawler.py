import fire
import gevent
import os
import urllib.request
from gevent import monkey
from queue import Queue, Empty

monkey.patch_all()


def generate_path_translator(ftp_root: str, base: str = None):
    if ftp_root.endswith('/'):
        ftp_root = ftp_root[:-1]
    if not base:
        _, base = os.path.split(ftp_root)

    def __translate(url: str):
        print(ftp_root)
        return os.path.join(base, os.path.relpath(url, ftp_root))

    return __translate


def read_from_url(url):
    request = urllib.request.Request(url)
    result = urllib.request.urlopen(request)
    content = result.read().decode(errors='ignore')
    return content


def save_to_loca(url, filename):
    folder, _ = os.path.split(filename)
    os.makedirs(folder, exist_ok=True)
    urllib.request.urlretrieve(url=url, filename=filename)


def expand(folders: Queue, files: Queue):
    try:
        while True:
            url = folders.get(block=True, timeout=20)
            print('Listing Folder {}'.format(url))
            content = read_from_url(url)
            lines = [line.strip() for line in content.strip().split('\n')][::-1]
            for line in lines:
                info = line.split()
                if len(info) != 9:
                    continue
                acl, filename = info[0], info[-1]
                suburl = os.path.join(url, filename)
                if acl.startswith('d'):
                    folders.put(suburl)
                elif acl.startswith('-'):
                    files.put(suburl)
    except Empty:
        pass


def download(files: Queue, translator):
    try:
        while True:
            url = files.get(block=True, timeout=60)
            print('Downloading File {}'.format(url))
            save_to_loca(url, translator(url))
    except Empty:
        pass


def crawl(url: str, base: str = None):
    files = Queue()
    folders = Queue()
    folders.put(url)
    translator = generate_path_translator(url, base)
    nums_of_expandors = 30
    nums_of_downloaders = 30
    expanders = [gevent.spawn(expand, folders, files) for _ in range(nums_of_expandors)]
    downloaders = [gevent.spawn(download, files, translator) for _ in range(nums_of_downloaders)]
    gevent.joinall(expanders)
    gevent.joinall(downloaders)


if __name__ == "__main__":
    fire.Fire()
