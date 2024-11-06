## aws-s3-downloader

### Requirement
- AWS cli 
- Python3 with tqdm package

### Usage
```text
usage: s3_download.py [-h] --bucket BUCKET --key KEY [--output-dir OUTPUT_DIR] [--output-prefix OUTPUT_PREFIX] [--num-parts NUM_PARTS] [--num-threads NUM_THREADS] [--resume]

Download a large file from S3 in multiple parts and concatenate them.

options:
  -h, --help            show this help message and exit
  --bucket BUCKET       The name of the S3 bucket.
  --key KEY             The path to the file in the S3 bucket.
  --output-dir OUTPUT_DIR
                        The directory to save downloaded parts and the final file. (default: './downloaded_parts')
  --output-prefix OUTPUT_PREFIX
                        The prefix for output part files. (default: 'output')
  --num-parts NUM_PARTS
                        The number of parts to split the file into. (default: 10)
  --num-threads NUM_THREADS
                        The number of threads to use for downloading. (default: 4)
  --resume              Resume the download if a previous configuration exists.
```

### Example
When downloading 1TB file at public location `s3://bucket_name/parent_dir/sub_dir/file.txt` 
```bash
python s3_download.py --bucket bucket_name --key "parent_dir/sub_dir/file.txt" --num-parts 4000 --num-threads 16 
```
