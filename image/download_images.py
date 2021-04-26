import time
import pandas as pd
import aiohttp
import aiofiles
import asyncio
from pathlib import Path
from tqdm import tqdm
import os


async def async_process_csv(input_file_path, base_dir, image_dir, by_group, chunksize):
    print("Hello!", flush=True)
    start = time.time()
    if os.path.exists(input_file_path) is False:
        print("Can't find the input file: ", input_file_path)
        return
    headers = {"user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}
    async with aiohttp.ClientSession(headers=headers) as session:
        for chunk in tqdm(pd.read_csv(input_file_path, chunksize=chunksize, encoding="utf-8"), desc="csv ... "):
            # print('=============', i, flush=True)
            # print(chunk, flush=True)
            await process_rows(session, chunk, base_dir, image_dir, by_group)
    end = time.time()
    print("Read csv with pandas: ", (end - start), "sec", flush=True)


async def process_rows(session, df, base_dir, image_dir, by_group):
    tasks = [process_row(row, session, base_dir, image_dir, by_group) for row in df.itertuples(index=False)]
    await asyncio.gather(*tasks)


async def process_row(row, session, base_dir, image_dir, by_group):
    image_path = "%s/%s/%s/%s" % (base_dir, image_dir, by_group, row.caption)
    if os.path.exists(image_path) is False:
        Path(image_path).mkdir(parents=True, exist_ok=True)
        print('created directory', image_path, flush=True)
    name = picture_name_of(row.image)
    if name is None:
        print('skipping ' + row.image, flush=True)
    else:
#         print('fetching ' + row.image, flush=True)
        await fetch(session, 'https:' + row.image, image_path + '/' + name)


def picture_name_of(value):
    image_name = value.rsplit('/', 1)
    try:
        if len(image_name) < 2:
            return None
        return image_name[1]
    except Exception as e:
        print('====>', value)
        raise e


async def fetch(session, url, full_image_path):
    try:
        if not Path(full_image_path).exists():
            async with session.get(url, ssl=False) as resp:
#                 print('fetching ... ... ' + url, resp.status, flush=True)
                if resp.status == 200:
                    async with aiofiles.open(full_image_path, mode='wb') as f:
                        await f.write(await resp.read())
                        await f.close()
                else:
                    print('fetching failed ... ... ' + url, resp.status, flush=True)
                await resp.release()
    except Exception as e:
        print('fetching exception ... ... ', url, e)


if __name__ == "__main__":
    asyncio.run(async_process_csv('./m_images/train/images.csv', '.', 'm_images', 'train', 20))
