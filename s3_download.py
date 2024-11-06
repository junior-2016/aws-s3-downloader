import os
import subprocess
import concurrent.futures
import json
from tqdm import tqdm
import argparse
import time
import threading

# Create a global lock for thread-safe progress bar updates
lock = threading.Lock()

def download_part(bucket, key, temp_dir, output_prefix, part_num, start_byte, end_byte, pbar):
    part_filename = os.path.join(temp_dir, f"{output_prefix}_part{part_num + 1}.blow5")
    range_header = f"bytes={start_byte}-{end_byte}"

    # Check if the part file already exists and has the expected size
    if os.path.exists(part_filename) and os.path.getsize(part_filename) >= (end_byte - start_byte + 1):
        with lock:
            pbar.update(end_byte - start_byte + 1)
        return True

    # Execute the aws s3api get-object command with --no-sign-request
    command = (
        f"aws s3api get-object --bucket {bucket} --key {key} "
        f"--range {range_header} {part_filename} --no-sign-request"
    )

    start_time = time.time()
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    end_time = time.time()

    if result.returncode != 0:
        print(f"Failed to download part {part_num + 1}: {result.stderr.decode()}")
        return False

    # Update the progress bar with the size of the downloaded part and elapsed time in a thread-safe way
    with lock:
        pbar.update(end_byte - start_byte + 1)
        speed = (end_byte - start_byte + 1) / (end_time - start_time)
        pbar.set_postfix({"Speed (B/s)": f"{speed:.2f}"})

    return True

def concatenate_parts(temp_dir, output_dir, output_prefix, original_filename, num_parts):
    output_file_path = os.path.join(output_dir, original_filename)
    print(f"Concatenating {num_parts} parts into {output_file_path}")

    with open(output_file_path, 'wb') as output_file:
        for part_num in range(num_parts):
            part_filename = os.path.join(temp_dir, f"{output_prefix}_part{part_num + 1}.blow5")
            with open(part_filename, 'rb') as part_file:
                output_file.write(part_file.read())

    print(f"Concatenation complete. File saved as {output_file_path}")

def download_s3_file_in_parts(bucket, key, output_dir, output_prefix, num_parts, num_threads, resume):
    # Create the temp directory for downloading parts
    temp_dir = os.path.join(output_dir, "temp_dir")
    os.makedirs(temp_dir, exist_ok=True)

    # Save user arguments to a JSON file in output_dir
    config_file = os.path.join(output_dir, "download_config.json")
    if not resume or not os.path.exists(config_file):
        config = {
            "bucket": bucket,
            "key": key,
            "output_dir": output_dir,
            "output_prefix": output_prefix,
            "num_parts": num_parts,
            "num_threads": num_threads
        }
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=4)
    else:
        # Load the configuration file for resuming
        with open(config_file, 'r') as f:
            config = json.load(f)
        # Ensure the loaded config matches the current parameters
        if (
            config["bucket"] != bucket or
            config["key"] != key or
            config["num_parts"] != num_parts or
            config["num_threads"] != num_threads
        ):
            raise ValueError("The provided arguments do not match the saved configuration.")

    # Get the file size
    command = f"aws s3api head-object --bucket {bucket} --key {key} --no-sign-request"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print("Error getting object metadata:", result.stderr)
        return

    # Extract the ContentLength (file size in bytes) from the output
    metadata = eval(result.stdout)
    file_size = metadata["ContentLength"]
    part_size = file_size // num_parts

    print(f"File size: {file_size} bytes")
    print(f"Downloading in {num_parts} parts, each part size: {part_size} bytes")

    # Progress bar setup
    with tqdm(total=file_size, desc="Downloading Parts", unit="B", unit_scale=True) as pbar:
        # Download each part in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for part_num in range(num_parts):
                start_byte = part_num * part_size
                if part_num == num_parts - 1:
                    end_byte = file_size - 1  # Last part goes to the end of the file
                else:
                    end_byte = start_byte + part_size - 1

                futures.append(
                    executor.submit(download_part, bucket, key, temp_dir, output_prefix, part_num, start_byte, end_byte, pbar)
                )

            # Wait for all parts to be downloaded
            concurrent.futures.wait(futures)

    # Concatenate all parts into the final output file
    original_filename = os.path.basename(key)
    concatenate_parts(temp_dir, output_dir, output_prefix, original_filename, num_parts)

    print("Download and concatenation complete. All parts downloaded and concatenated successfully.")

# Main function to handle argument parsing
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download a large file from S3 in multiple parts and concatenate them.")
    parser.add_argument("--bucket", required=True, help="The name of the S3 bucket.")
    parser.add_argument("--key", required=True, help="The path to the file in the S3 bucket.")
    parser.add_argument("--output-dir", default="./downloaded_parts", help="The directory to save downloaded parts and the final file. (default: './downloaded_parts')")
    parser.add_argument("--output-prefix", default="output", help="The prefix for output part files. (default: 'output')")
    parser.add_argument("--num-parts", type=int, default=10, help="The number of parts to split the file into. (default: 10)")
    parser.add_argument("--num-threads", type=int, default=4, help="The number of threads to use for downloading. (default: 4)")
    parser.add_argument("--resume", action="store_true", help="Resume the download if a previous configuration exists.")

    args = parser.parse_args()

    # Call the main download function with parsed arguments
    download_s3_file_in_parts(
        bucket=args.bucket,
        key=args.key,
        output_dir=args.output_dir,
        output_prefix=args.output_prefix,
        num_parts=args.num_parts,
        num_threads=args.num_threads,
        resume=args.resume
    )
