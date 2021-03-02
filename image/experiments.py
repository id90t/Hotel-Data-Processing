import time
import pandas as pd
import aiohttp
import aiofiles
import asyncio
from pathlib import Path
from tqdm import tqdm
import os


# todo: progress bar to show progress
# todo: setup right directory structure
# todo: move some of the code to unit tests once it is setup


def read_csv(filename, chunksize):
    print("Hello!")
    start = time.time()
    for chunk in pd.read_csv(filename, chunksize=chunksize, encoding="utf-8"):
        # chunk['photo'] = chunk['image'].map(picture_name_of)
        print(chunk['image'].drop_duplicates().tolist())
        print('=============')
    end = time.time()
    print("Read csv with pandas: ", (end - start), "sec")


def simulate_process_csv(input_file_path, base_dir, image_dir, by_group, chunksize):
    print("Hello!")
    start = time.time()
    if os.path.exists(input_file_path) is False:
        print("Can't find the input file: ", input_file_path)
        return

    i = 0
    for chunk in tqdm(pd.read_csv(input_file_path, chunksize=chunksize, encoding="utf-8"), desc="csv ... "):
        # print('=============', i)
        asyncio.run(simulate_main(chunk, base_dir, image_dir, by_group))
        i = i + 1
    end = time.time()
    print("Read csv with pandas: ", (end - start), "sec")


def process_csv(input_file_path, base_dir, image_dir, by_group, chunksize):
    print("Hello!", flush=True)
    start = time.time()
    if os.path.exists(input_file_path) is False:
        print("Can't find the input file: ", input_file_path)
        return
    i = 0
    # for chunk in tqdm(pd.read_csv(input_file_path, chunksize=chunksize, encoding="utf-8"), desc="csv ... "):
    for chunk in pd.read_csv(input_file_path, chunksize=chunksize, encoding="utf-8"):
        print('=============', i, flush=True)
        # print(chunk, flush=True)
        asyncio.run(process_main(chunk, base_dir, image_dir, by_group))
        i = i + 1
    end = time.time()
    print("Read csv with pandas: ", (end - start), "sec", flush=True)


async def async_process_csv(input_file_path, base_dir, image_dir, by_group, chunksize):
    print("Hello!", flush=True)
    start = time.time()
    if os.path.exists(input_file_path) is False:
        print("Can't find the input file: ", input_file_path)
        return
    i = 0
    headers = {"user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}
    async with aiohttp.ClientSession(headers=headers) as session:
        # for chunk in tqdm(pd.read_csv(input_file_path, chunksize=chunksize, encoding="utf-8"), desc="csv ... "):
        for chunk in pd.read_csv(input_file_path, chunksize=chunksize, encoding="utf-8"):
            print('=============', i, flush=True)
            # print(chunk, flush=True)
            await async_process_main(session, chunk, base_dir, image_dir, by_group)
            i = i + 1
    end = time.time()
    print("Read csv with pandas: ", (end - start), "sec", flush=True)


def picture_name_of(value):
    image_name = value.rsplit('/', 1)
    return image_name[1]


async def fetch(session, url, full_image_path):
    try:
        async with session.get(url) as resp:
            # print('fetching ... ... ' + url, resp.status, flush=True)
            if resp.status == 200:
                async with aiofiles.open(full_image_path, mode='wb') as f:
                    await f.write(await resp.read())
                    await f.close()
            else:
                print('fetching failed ... ... ' + url, resp.status, flush=True)
    except:
        print('fetching exception ... ... ', url)


async def simulate_fetch(url, full_image_path):
    # print('simulate_fetch', url, full_image_path)
    f = asyncio.Future()
    f.set_result(full_image_path)
    return f


async def simulate_main(df, base_dir, image_dir, by_group):
    tasks = []
    for row in tqdm(df.itertuples(index=False), desc="downloads ... "):
        image_path = "%s/%s/%s/%s" % (base_dir, image_dir, by_group, row.caption)
        if os.path.exists(image_path) is False:
            Path(image_path).mkdir(parents=True, exist_ok=True)
            # print('created directory', image_path, flush=True)
        tasks.append(await simulate_fetch(row.image, image_path + '/' + picture_name_of(row.image)))
    await asyncio.gather(*tasks)
    # print('simulate_main', flush=True)


async def async_process_main(session, df, base_dir, image_dir, by_group):
    tasks = []
    # for row in tqdm(df.itertuples(index=False), desc="downloads ... "):
    for row in df.itertuples(index=False):
        image_path = "%s/%s/%s/%s" % (base_dir, image_dir, by_group, row.caption)
        if os.path.exists(image_path) is False:
            Path(image_path).mkdir(parents=True, exist_ok=True)
            print('created directory', image_path, flush=True)
        tasks.append(fetch(session, 'http:' + row.image, image_path + '/' + picture_name_of(row.image)))
    await asyncio.gather(*tasks)


async def process_main(df, base_dir, image_dir, by_group):
    headers = {"user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = []
        # for row in tqdm(df.itertuples(index=False), desc="downloads ... "):
        for row in df.itertuples(index=False):
            image_path = "%s/%s/%s/%s" % (base_dir, image_dir, by_group, row.caption)
            if os.path.exists(image_path) is False:
                Path(image_path).mkdir(parents=True, exist_ok=True)
                print('created directory', image_path, flush=True)
            tasks.append(fetch(session, 'http:' + row.image, image_path + '/' + picture_name_of(row.image)))
        await asyncio.gather(*tasks)


# read_csv('image_test.csv', 5)
# asyncio.run(simulate_process_csv('image_test.csv', 5))
# asyncio.run(process_csv('image_test.csv', 5))

# process_csv('./hotel_images/tests/image_test.csv', '.', 'hotel_images', 'test', 25)

# simulate_process_csv('./tests/images.csv', './tests', 'test_images', 'test', 20)

# process_csv('./hotel_images/test/images.csv', '.', 'hotel_images', 'test', 25)

asyncio.run(async_process_csv('./hotel_images/test/images.csv', '.', 'hotel_images', 'test', 25))
