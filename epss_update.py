import requests
import gzip
import shutil
import os
import datetime

def download_and_extract_epss():
    """
    Downloads, extracts, and saves the latest EPSS data.
    """
    epss_url = "https://epss.empiricalsecurity.com/epss_scores-current.csv.gz"
    output_dir = "."
    
    gz_file_name = "epss_scores-current.csv.gz"
    gz_file_path = os.path.join(output_dir, gz_file_name)
    
    extracted_file_name = "epss_scores-current.csv"
    extracted_file_path = os.path.join(output_dir, extracted_file_name)

    try:
        print(f"Downloading {epss_url}...")
        with requests.get(epss_url, stream=True) as r:
            r.raise_for_status()
            with open(gz_file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Download complete.")

        print("Decompressing the file...")
        with gzip.open(gz_file_path, 'rb') as f_in:
            with open(extracted_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Decompression complete.")

        os.remove(gz_file_path)
        print("Cleaned up .gz file.")

    except Exception as e:
        print(f"An error occurred: {e}")
        return False

    return True

if __name__ == "__main__":
    download_and_extract_epss()
