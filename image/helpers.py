"""Module to process supplier data

Contains functions to help process images.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

def download_photo(img_url, group_type, category, filename):
    try:
        file_path = "./hotelImages/%s/%s/%s" % (group_type, category, filename)
        urlretrieve(img_url, file_path)
    except:
        print(img_url)
        return False
    return True

def download_photo_row(index, row):
    image_name = row['image'].rsplit('/', 1)
    return download_photo(img_url='http:' + row['image'], group_type='valid', category=row['caption'], filename= "%d.%s" % (index, image_name[1]))

def analyze_photo_name(row):
    image_name = row['image'].rsplit('/', 1)
    return image_name[1]

def progressive_download_photo(pbar, index, row):
    download_photo_row(index, row)
    pbar.update(1)