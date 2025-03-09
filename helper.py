import os
import shutil

def remove_dir_if_exists(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)