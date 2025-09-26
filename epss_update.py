import requests
import gzip
import shutil
import os
import csv
import io
import sys

def download_and_extract_epss():
    """Downloads, extracts, and saves the latest EPSS data."""
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

        # CSV分割処理
        chunk_csv_file(extracted_file_path)
        
        # 分割されたファイルのみを保持するため、元の巨大なCSVファイルとgzファイルを削除
        os.remove(extracted_file_path)
        os.remove(gz_file_path)
        print("Cleaned up original files.")

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1) # エラー時即座に終了

    return True

def chunk_csv_file(input_file_path):
    """CSVファイルをサイズに基づいて分割する"""
    MAX_SIZE_BYTES = 95 * 1024 * 1024 
    OUTPUT_BASE_NAME = 'epss_part'
    
    with open(input_file_path, 'r', encoding='utf-8', newline='') as infile:
        reader = csv.reader(infile)
        header = next(reader) # ヘッダー行を読み込む
        
        chunk_index = 1
        output_name = f'{OUTPUT_BASE_NAME}_{chunk_index}.csv'
        out_file_path = os.path.join('.', output_name)
        
        # 現在のチャンクデータをメモリに保持し、サイズチェックに利用
        current_chunk_data = [header] 
        
        # 分割後の最初の出力ファイルを作成
        with open(out_file_path, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header) # ヘッダーを書き込む

            for row in reader:
                
                # 行を一時的にバッファに書き出し、サイズを概算
                output = io.StringIO()
                temp_writer = csv.writer(output)
                temp_writer.writerow(row)
                row_size = len(output.getvalue().encode('utf-8'))

                # サイズチェック
                if os.path.getsize(out_file_path) + row_size >= MAX_SIZE_BYTES:
                    print(f"Saved {output_name}")
                    
                    # 次のチャンクへ
                    chunk_index += 1
                    output_name = f'{OUTPUT_BASE_NAME}_{chunk_index}.csv'
                    out_file_path = os.path.join('.', output_name)
                    
                    # 新しいファイルにヘッダーを書き込み
                    with open(out_file_path, 'w', encoding='utf-8', newline='') as new_outfile:
                         new_writer = csv.writer(new_outfile)
                         new_writer.writerow(header)
                         new_writer.writerow(row) # 現在の行を書き込む
                else:
                    # 制限内に収まる場合、現在のファイルに書き込む
                    writer.writerow(row)
                         
    print(f"Finished splitting CSV into {chunk_index} parts.")

if __name__ == "__main__":
    download_and_extract_epss()
