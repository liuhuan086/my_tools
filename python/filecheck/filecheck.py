import argparse
import os
from queue import Queue
from threading import Thread

file_keyword_dic = {}
keywords = [
    'user', "username", "password", "passwd", "secret", "access", "admin", "Admin", "root"
]


def check(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                line = line.strip()
                for k in keywords:
                    if k in line:
                        if filename not in file_keyword_dic.keys():
                            file_keyword_dic[filename] = [k]
                        else:
                            file_keyword_dic[filename].append(k)
    except Exception:
        pass


def put_file(base_path, queue: Queue):
    abs_path = os.path.abspath(base_path)
    if os.path.isdir(abs_path):
        for root, ds, fs in os.walk(abs_path):
            for f in fs:
                fullname = os.path.join(root, f)
                queue.put_nowait(fullname)
    else:
        check(abs_path)


def get_file(queue: Queue):
    ts = []
    while not queue.empty():
        fullname = queue.get_nowait()
        t = Thread(target=check, args=(fullname,))
        ts.append(t)
        t.start()

    for t in ts:
        t.join()


def run(base_path):
    queue = Queue()
    put_file(base_path, queue)
    get_file(queue)
    print(file_keyword_dic)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='敏感内容检测')
    parser.add_argument('--path', dest='path', help='需要检测的文件夹路径')
    args = parser.parse_args()
    path = args.path
    run(path)
    # eg:
    # python3 filecheck.py --path ./
    # python3 filecheck.py --path /opt/proj
    # python3 filecheck.py --path /opt/proj/test.sh
