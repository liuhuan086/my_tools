import os
import shutil
from distutils.core import setup
from Cython.Build import cythonize


def _build_so(path, build_dir):
    """
    :param path: The path is dir or python file.
    :param build_dir: The path that the .os file you want to save.
    :return:
    """
    if not os.path.exists(path):
        print("path not exists.")
        return

    path = os.path.abspath(path)

    if os.path.isdir(path):
        for cur_dir, dirs, files in os.walk(path):
            for file in files:
                cur_path = os.path.join(cur_dir, file)
                if '.git' in cur_path or '.idea' in cur_path or 'build' in cur_path:
                    continue

                elif not cur_path.endswith('.py'):
                    continue

                elif 'setup.py' in cur_path:
                    continue

                build_path = cur_path.replace(path, build_dir)
                folder = os.path.dirname(build_path)

                if not os.path.exists(folder):
                    os.makedirs(folder)

                setup(ext_modules=cythonize([cur_path], language_level=3), script_args=["build_ext", "-b", folder])

    else:
        if not os.path.exists(build_dir):
            os.makedirs(build_dir)

        setup(ext_modules=cythonize([path], language_level=3), script_args=["build_ext", "-b", build_dir])


def clear(input_path, output_path):
    """
    :param input_path: pass.
    :param output_path: The file that you want save to.

    Rename the .so file and delete the temp dir.
    """
    skip_path = os.path.join(os.path.abspath(input_path), 'build')
    build_path = os.path.abspath(output_path)

    cur_path = os.getcwd()

    if not build_path == skip_path:
        # if build path is not the default path (./build), then delete the default path
        # if is the same path, skip.
        shutil.rmtree(os.path.join(cur_path, 'build'))

    for cur_dir, dirs, files in os.walk(input_path):
        for file in files:
            if file.endswith('.c'):
                cur_path = os.path.join(cur_dir, file)
                os.remove(cur_path)

    for cur_dir, dirs, files in os.walk(output_path):
        if 'temp.' in cur_dir:
            shutil.rmtree(cur_dir)

        for file in files:
            if file.endswith('.so'):
                filename = file.split('.')[0]
                old_so_path = os.path.join(cur_dir, file)
                new_so_path = os.path.join(cur_dir, f'{filename}.so')
                os.rename(old_so_path, new_so_path)


def build_so(input_path, output_path):
    _build_so(input_path, output_path)
    clear(input_path, output_path)


if __name__ == '__main__':
    in_path = r'D:\test1'
    out_path = r'D:\test2'
    build_so(in_path, out_path)
