from src.utils.logs import logging
from concurrent.futures import ThreadPoolExecutor
import os, boto3

data_path = os.path.join(os.path.dirname(__file__), 'data')
files_list = (entry.path for entry in os.scandir(data_path) if entry.is_file() and entry.name.endswith('.jsonl'))
logger = get_logger(__name__)
# data_store = dot env shit here
# os.getenv("data_store_bucket")
# do later ^ replace: BUCKET, KEY

# put CHUNK_SIZE=1024*1024*8 inside .env (8MB chunks)  

"""
source/cite: https://github.com/9001/copyparty

copyparty uses a protocol called UP2K (Upload 2000) to maximise throughput. the way it does this is instead of fixed-size chunks, 
copyparty calculates an "optimal" chunk size based on the total file size. it starts at 1 MiB per chunk. if the file is large enough
to create more than 256 chunks, it increases the chunk size (1.5M, 2M, 4M. this goes up to 32M). this ensures the overhead of
managing chunks doesnt overwhelm the browser or server. before uploading bytes, the client hashes every chunk (usually using xxh128 
for speed or sha512 for security). the client sends a list of hashes to the server then the server checks its database. if the file
(or specific chunks) already exist, the server tells the client it already has them. On top of this, he client opens multiple
concurrent POST connections (4-8). each connection grabs a "pending" chunk and sends it independently. this is faster because standard
TCP connections can "throttle" or stall due to congestion. if one connection slows down, the others keep the "pipe" full.
"""

"""

def _upload_part(upload_id, part_number, chunk_data):
    response = data_store.upload_part(Bucket=BUCKET, Key=KEY, PartNumber=part_number,UploadId=upload_id, Body=chunk_data) # boto.client (boto3 import)
    return {'PartNumber': part_number, 'ETag': response['ETag']}

def _parallel_data_store_upload():
    res = data_store.create_multipart_upload(Bucket=BUCKET,Key=KEY)
    upload_id = res['UploadId']
    
    parts = []
    file_size = os.path.getsize(FILE_PATH)
    
    # uploading chunks
    with open(FILE_PATH, 'rb') as f:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            part_num = 1
            while True:
                data = f.read(CHUNK_SIZE)
                if not data: break
                futures.append(executor.submit(upload_part, upload_id, part_num, data))
                part_num += 1
            
            parts = [f.result() for f in futures]

    parts.sort(key=lambda x: x['PartNumber'])
    data_store.complete_multipart_upload(Bucket=BUCKET, Key=KEY, UploadId=upload_id,MultipartUpload={'Parts': parts})
    logger.info(f"[upload id::{upload_id}] [parts::{parts}]")

"""