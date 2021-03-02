#!/usr/bin/python

import getopt
import sys
import asyncio

from download_images import async_process_csv


def main(argv):
    input_file_path = None
    base_dir = '.'
    image_dir = 'hotel_images'
    by_group = 'test'
    chunksize = 25
    try:
        opts, args = getopt.getopt(argv, "hi:b:o:g:s:", ["input_file_path=", "base_dir=", "image_dir=", "by_group=", "chunksize="])
    except getopt.GetoptError:
        print('run_image_download.py -i <input_file_path> -b <base_dir> -o <image_dir> -g <by_group> -s <chunksize>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('run_image_download.py -i <input_file_path> -b <base_dir> -o <image_dir> -g <by_group> -s <chunksize>')
            sys.exit()
        elif opt in ("-i", "--input_file_path"):
            input_file_path = arg
        elif opt in ("-b", "--base_dir"):
            base_dir = arg
        elif opt in ("-g", "--by_group"):
            by_group = arg
        elif opt in ("-o", "--image_dir"):
            image_dir = arg
        elif opt in ("-s", "--chunksize"):
            chunksize = int(arg)
    print("run_image_download.py -i %s -b %s -o %s -g %s -s %d" % (input_file_path, base_dir, image_dir, by_group, chunksize))
    asyncio.run(async_process_csv(input_file_path, base_dir, image_dir, by_group, chunksize))


if __name__ == "__main__":
    main(sys.argv[1:])
