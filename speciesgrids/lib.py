import logging
import os


def clear_directory(path):
    logging.info(f"Clearing {path}")
    files = os.listdir(path)
    for file in files:
        file_path = os.path.join(path, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
