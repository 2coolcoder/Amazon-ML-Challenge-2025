import re
import os
import pandas as pd
import multiprocessing
from time import time as timer
from tqdm import tqdm
import numpy as np
from pathlib import Path
from functools import partial
import requests
import urllib
from collections.abc import Iterable

# def download_image(image_link, savefolder,filename):
#     if(isinstance(image_link, str)):
#         # filename = Path(image_link).name
#         print(filename)
#         image_save_path = os.path.join(savefolder, filename)
#         if(not os.path.exists(image_save_path)):
#             try:
#                 urllib.request.urlretrieve(image_link, image_save_path)    
#             except Exception as ex:
#                 print('Warning: Not able to download - {}\n{}'.format(image_link, ex))
#         else:
#             return
#     return

# def download_images(image_links, download_folder,sample_ids):
#     if not os.path.exists(download_folder):
#         os.makedirs(download_folder)
#     results = []
#     download_image_partial = partial(download_image, savefolder=download_folder)
#     with multiprocessing.Pool(100) as pool:
#         for result in tqdm(pool.imap(download_image_partial, image_links), total=len(image_links)):
#             results.append(result)
#         pool.close()
#         pool.join()

def download_image(image_link, savefolder, filename):
    if not isinstance(image_link, str) and isinstance(image_link, Iterable):
        for url, name in zip(image_link, filename):
            download_image(url, savefolder, name)  # recurse in scalar mode
        return

    # Scalar mode
    if not isinstance(image_link, str) or not image_link.strip():
        return

    os.makedirs(savefolder, exist_ok=True)

    # Ensure we save as {sample_id}{ext_from_url}    
    fname = f"{filename}"
    image_save_path = os.path.join(savefolder, fname)

    if os.path.exists(image_save_path):
        return  # already there

    try:
        # basic download with a User-Agent to avoid some throttling issues
        req = urllib.request.Request(image_link, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp, open(image_save_path, "wb") as f:
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                f.write(chunk)
    except Exception as ex:
        print('Warning: Not able to download - {}\n{},{}'.format(image_link, ex,fname))
    return

def download_images(image_links, download_folder, sample_ids):
    """
    Parallel version that saves each image as {sample_id}{ext}.
    """
    os.makedirs(download_folder, exist_ok=True)
    links = list(image_links)
    ids = list(sample_ids)

    # Build argument tuples for starmap → calls download_image(url, folder, id)
    args = [(url, download_folder, str(sid)) for url, sid in zip(links, ids)]

    # Tune processes if needed; 100 is your original choice
    with multiprocessing.Pool(processes=min(100, len(args) or 1)) as pool:
        for _ in tqdm(pool.starmap(download_image, args), total=len(args)):
            pass
        pool.close()
        pool.join()