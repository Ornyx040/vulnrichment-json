import json
import glob
import datetime
import os
import sys

# 最大ファイルサイズ（約95MB）を設定。GitHubの100MB制限を下回る安全圏。
MAX_SIZE_BYTES = 95 * 1024 * 1024 
OUTPUT_BASE_NAME = 'vulnrichment_part'

def download_and_combine_data():
    """データをダウンロードし、結合・ソート・分割するメイン関数"""
    
    json_files = glob.glob('vulnrichment/**/*.json', recursive=True)

    all_data = []
    for file_path in json_files:
        try:
            if os.path.basename(file_path) == 'cve.json':
                continue
            with open(file_path, encoding='utf-8') as f:
                all_data.append(json.load(f))
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            sys.exit(1) # エラー時即座に終了

    # 各レコードのトップレベルのキーの数で降順ソート
    all_data_sorted = sorted(all_data, key=lambda x: len(x.keys()), reverse=True)
    
    chunk_data_and_save(all_data_sorted)


def chunk_data_and_save(all_data):
    """メモリ上でのサイズ概算に基づき、データを分割して保存する"""
    
    current_chunk = []
    current_size = 0
    chunk_index = 1
    
    # JSONのエンコーディングと圧縮を考慮し、バイト数を概算
    # separators=(',', ':') が適用されるものとして計算
    
    for record in all_data:
        # レコードをJSON文字列に変換し、バイトサイズを取得
        record_string = json.dumps(record, ensure_ascii=False, separators=(',', ':'))
        record_size = len(record_string.encode('utf-8'))
        
        # 配列のカンマ区切り (約1バイト) も考慮
        if current_chunk:
            size_to_add = record_size + 1 
        else:
            size_to_add = record_size
            
        # サイズチェック
        # NOTE: 最終的なファイルには外側の配列[]の2バイトと改行が含まれるため、これはあくまで近似値です
        if current_size + size_to_add >= MAX_SIZE_BYTES:
            
            # --- チャンク保存 ---
            output_name = f'{OUTPUT_BASE_NAME}_{chunk_index}.json'
            with open(output_name, 'w', encoding='utf-8') as out_file:
                # チャンク全体をリストとして保存
                json.dump(current_chunk, out_file, ensure_ascii=False, separators=(',', ':')) 

            print(f"Saved {output_name} with {len(current_chunk)} records. Size: {round(current_size / (1024*1024), 2)} MB")
            
            # 次のチャンクを開始
            current_chunk = [record]
            current_size = record_size
            chunk_index += 1
        else:
            # チャンクに追加
            current_chunk.append(record)
            current_size += size_to_add

    # 最後のチャンクを保存
    if current_chunk:
        output_name = f'{OUTPUT_BASE_NAME}_{chunk_index}.json'
        with open(output_name, 'w', encoding='utf-8') as out_file:
            json.dump(current_chunk, out_file, ensure_ascii=False, separators=(',', ':'))
        print(f"Saved final {output_name} with {len(current_chunk)} records. Size: {round(current_size / (1024*1024), 2)} MB")

    print(f"Total parts created: {chunk_index}")


if __name__ == "__main__":
    download_and_combine_data()
