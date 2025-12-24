import pandas as pd
import numpy as np
import os

# ==========================================
# 1. 設定檔案清單 (Load Multiple Files)
# ==========================================
file_list = [
    '110年度A1交通事故資料.csv',
    '110年度A2交通事故資料(110年1月-6月).csv',
    '110年度A2交通事故資料(110年7月-12月).csv'
]

data_frames = []

print(" 開始讀取並合併檔案...")

for file in file_list:
    if os.path.exists(file):
        print(f"  正在讀取: {file} ...")
        # 政府資料常見 big5 或 utf-8，這邊做個防呆
        try:
            # low_memory=False 避免檔案過大時 pandas 發出警告
            temp_df = pd.read_csv(file, encoding='utf-8', low_memory=False)
        except UnicodeDecodeError:
            temp_df = pd.read_csv(file, encoding='big5', low_memory=False)
        
        # 為了分辨資料來源，我們可以加一個標籤 (可選)
        if 'A1' in file:
            temp_df['事故類型'] = 'A1(死亡)'
        else:
            temp_df['事故類型'] = 'A2(受傷)'
            
        data_frames.append(temp_df)
    else:
        print(f" 找不到檔案: {file}，請確認檔名是否正確")

# 將三個檔案垂直合併 (Concat)
if len(data_frames) > 0:
    df_all = pd.concat(data_frames, ignore_index=True)
    print(f" 合併完成！全台總資料筆數: {len(df_all)}")
else:
    print(" 沒有讀取到任何資料，程式結束")
    exit()

# ==========================================
# 2. 聚焦台南 (Filter Scope)
# ==========================================
print("\n 正在篩選「臺南市」資料...")
# 確保欄位名稱正確，通常是 '發生地點'
target_city = '臺南'
df_tainan = df_all[df_all['發生地點'].astype(str).str.contains(target_city, na=False)].copy()

print(f"台南市資料筆數: {len(df_tainan)}")

# ==========================================
# 3. 資料清理 (Data Cleaning)
# ==========================================
print("\n 開始資料清理...")

# 先檢查資料欄位
print(f"資料欄位: {list(df_tainan.columns)}")

# 這份資料的欄位比較簡單，主要包含：發生時間、發生地點、死亡受傷人數、車種、經度、緯度
# 不需要進行日期時間轉換

# --- 經緯度處理 (重要：Tableau 畫地圖需要) ---
# 110年的資料通常已經是 WGS84 (經緯度)，但偶爾會有髒資料
def clean_lat(val):
    try:
        v = float(val)
        # 台灣緯度約在 21~26 之間
        return v if 21 <= v <= 26 else np.nan
    except:
        return np.nan

def clean_lon(val):
    try:
        v = float(val)
        # 台灣經度約在 119~123 之間
        return v if 119 <= v <= 123 else np.nan
    except:
        return np.nan

# 檢查欄位名稱，有時候叫 '緯度'/'經度'，有時候可能有差異
if '緯度' in df_tainan.columns and '經度' in df_tainan.columns:
    df_tainan['clean_lat'] = df_tainan['緯度'].apply(clean_lat)
    df_tainan['clean_lon'] = df_tainan['經度'].apply(clean_lon)
    
    # 移除無法定位的資料 (為了地圖視覺化)
    before_drop = len(df_tainan)
    df_tainan = df_tainan.dropna(subset=['clean_lat', 'clean_lon'])
    after_drop = len(df_tainan)
    print(f"座標清理：移除了 {before_drop - after_drop} 筆無效座標資料")

# ==========================================
# 4. 輸出結果 (Export)
# ==========================================
output_filename = '110_tainan_accidents_cleaned.csv'
df_tainan.to_csv(output_filename, index=False, encoding='utf-8-sig')

print(f"\n 處理完成！檔案已儲存為: {output_filename}")
