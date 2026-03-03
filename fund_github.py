# !pip install requests --quiet
# !pip install numpy pandas quantstats --quiet
# !pip install scikit-learn --quiet

import requests
import pandas as pd
import numpy as np
import csv
import time
import os
import shutil
import quantstats as qs
import builtins
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import concurrent.futures

# ต้องการแสดงผลแบบเต็มโดยไม่ขึ้นบรรทัดใหม่
pd.set_option('display.max_colwidth', None)  # แสดงผลข้อความเต็ม ไม่ตัด
pd.set_option('display.width', 1000)  # กำหนดความกว้างของตาราง
pd.set_option('display.max_columns', None)  # ให้แสดงทุกคอลัมน์

# กำหนด path
local_path = "./"
local_path_nav = "./data_nav/"
local_path_opt = "./data_opt/"

# ลบ folder + file ที่เก็บไฟล์เดิมไว้และสร้างใหม่
# ระบุพาธโฟลเดอร์ที่ต้องการลบและสร้างใหม่
target_folder_1 = local_path+'data_nav'
target_folder_2 = local_path+'data_opt'

# ตรวจสอบว่าโฟลเดอร์มีอยู่หรือไม่
if os.path.exists(target_folder_1):
    shutil.rmtree(target_folder_1)  # ลบโฟลเดอร์ทั้งหมด
    print(f"ลบโฟลเดอร์ {target_folder_1} เรียบร้อยแล้ว")

# สร้างโฟลเดอร์ใหม่
os.makedirs(target_folder_1)
print(f"สร้างโฟลเดอร์ใหม่ที่ {target_folder_1} สำเร็จแล้ว")

# ตรวจสอบว่าโฟลเดอร์มีอยู่หรือไม่
if os.path.exists(target_folder_2):
    shutil.rmtree(target_folder_2)  # ลบโฟลเดอร์ทั้งหมด
    print(f"ลบโฟลเดอร์ {target_folder_2} เรียบร้อยแล้ว")

# สร้างโฟลเดอร์ใหม่
os.makedirs(target_folder_2)
print(f"สร้างโฟลเดอร์ใหม่ที่ {target_folder_2} สำเร็จแล้ว")

# ระบุพาธไฟล์ที่ต้องการลบ
target_file_1 = local_path+'finnomena_fund_list.csv'
target_file_2 = local_path+'overview.csv'
target_file_3 = local_path+'performance.csv'
target_file_4 = local_path+'portfolio.csv'
target_file_5 = local_path+'fee.csv'
target_file_6 = local_path+'merged_fund_for_optimzed.csv'

# ตรวจสอบว่าไฟล์ 1 มีอยู่หรือไม่
if os.path.exists(target_file_1):
    os.remove(target_file_1)  # ลบไฟล์
    print(f"ลบไฟล์ {target_file_1} เรียบร้อยแล้ว")
else:
    print(f"ไม่พบไฟล์ {target_file_1} ที่ต้องการลบ")

# ตรวจสอบว่าไฟล์ 2 มีอยู่หรือไม่
if os.path.exists(target_file_2):
    os.remove(target_file_2)  # ลบไฟล์
    print(f"ลบไฟล์ {target_file_2} เรียบร้อยแล้ว")
else:
    print(f"ไม่พบไฟล์ {target_file_2} ที่ต้องการลบ")

# ตรวจสอบว่าไฟล์ 3 มีอยู่หรือไม่
if os.path.exists(target_file_3):
    os.remove(target_file_3)  # ลบไฟล์
    print(f"ลบไฟล์ {target_file_3} เรียบร้อยแล้ว")
else:
    print(f"ไม่พบไฟล์ {target_file_3} ที่ต้องการลบ")

# ตรวจสอบว่าไฟล์ 4 มีอยู่หรือไม่
if os.path.exists(target_file_4):
    os.remove(target_file_4)  # ลบไฟล์
    print(f"ลบไฟล์ {target_file_4} เรียบร้อยแล้ว")
else:
    print(f"ไม่พบไฟล์ {target_file_4} ที่ต้องการลบ")

# ตรวจสอบว่าไฟล์ 5 มีอยู่หรือไม่
if os.path.exists(target_file_5):
    os.remove(target_file_5)  # ลบไฟล์
    print(f"ลบไฟล์ {target_file_5} เรียบร้อยแล้ว")
else:
    print(f"ไม่พบไฟล์ {target_file_5} ที่ต้องการลบ")

# ตรวจสอบว่าไฟล์ 6 มีอยู่หรือไม่
if os.path.exists(target_file_6):
    os.remove(target_file_6)  # ลบไฟล์
    print(f"ลบไฟล์ {target_file_6} เรียบร้อยแล้ว")
else:
    print(f"ไม่พบไฟล์ {target_file_6} ที่ต้องการลบ")

# ดึง fund list ทั้งหมด
# ดึง list ข้อมูลทั้งหมดก่อน
# URL ที่ต้องการดึงข้อมูล
url = "https://www.finnomena.com/fn3/api/fund/public/list/"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.finnomena.com/fund/list',
}

try:
    # ดึงข้อมูลจาก API
    response = requests.get(url, timeout=30, headers=headers, verify=False)
    response.raise_for_status()
    funds_data = response.json()

    # ตรวจสอบโครงสร้าง JSON กันชนชื่อ list โดนทับ
    if not isinstance(funds_data, builtins.list):
        raise ValueError(f"โครงสร้างข้อมูลไม่ถูกต้อง: ต้องเป็น list แต่ได้ {type(funds_data)}")

    # แปลงข้อมูล JSON เป็น DataFrame
    df = pd.DataFrame(funds_data)

    # ตรวจสอบคอลัมน์ที่ต้องการ
    expected_columns = ["id", "short_code", "name_th", "aimc_category_id", "is_nter_pick"]
    missing = [c for c in expected_columns if c not in df.columns]
    if missing:
        raise ValueError(f"ข้อมูลขาดคอลัมน์: {missing}")

    # บันทึกข้อมูลลงไฟล์ CSV
    file_path = f"{local_path}finnomena_fund_list.csv"
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"ข้อมูลถูกบันทึกในไฟล์ {file_path} เรียบร้อยแล้ว")

except requests.exceptions.RequestException as req_err:
    print(f"เกิดข้อผิดพลาดในการเรียก API: {req_err}")
except ValueError as val_err:
    print(f"เกิดข้อผิดพลาดของข้อมูล: {val_err}")
except Exception as e:
    print(f"เกิดข้อผิดพลาดที่ไม่คาดคิด: {e}")

# อ่านไฟล์ export (finnomena_fund_list)
file_path = local_path+'finnomena_fund_list.csv'
df = pd.read_csv(file_path, header=None)
header_values = df.iloc[0].tolist()
print("ค่าใน header:", header_values)

# อ่านไฟล์ csv และเก็บเป็น list ของกองทั้งหมด
file_path = local_path + 'finnomena_fund_list.csv'
df = pd.read_csv(file_path)

all_list = []
all_list = df['id'].tolist()
# all_list = df.loc[df['aimc_category_id'] == 'LC00002468', 'id'].tolist() # แบบมีเงื่อนไข
print(all_list)
print("จำนวนกองทุนทั้งหมด:", len(all_list))

unique_aimc_category_ids = []
# ดึงเฉพาะค่า aimc_category_id ที่ไม่ซ้ำ
unique_aimc_category_ids = df['aimc_category_id'].dropna().unique().tolist()
print(unique_aimc_category_ids)
print("จำนวนประเภทแยกตาม aimc ทั้งหมด:", len(unique_aimc_category_ids))

max_retries = 3   # จำนวนครั้งสูงสุดที่จะลองใหม่
retry_delay = 5   # หน่วงเวลาก่อนลองใหม่ (วินาที)

def fetch_data_concurrently(fund_code, endpoint_suffix, max_retries, retry_delay):
    """ดึงข้อมูล API สำหรับกองทุนเดียว พร้อมจัดการ retry"""
    url = f"https://www.finnomena.com/fn3/api/fund/v2/public/funds/{fund_code}{endpoint_suffix}"
    
    for attempts in range(max_retries):
        try:
            response = requests.get(url, timeout=30, headers=headers, verify=False)
            
            # การจัดการ Rate Limit (HTTP 429)
            if response.status_code == 429:
                print(f"Too many requests (429) for {fund_code}. รอ 300s ก่อนลองใหม่...")
                time.sleep(300) 
                continue # ลองใหม่อีกครั้งโดยไม่นับเป็นความพยายามที่ล้มเหลว
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"ไม่สามารถโหลดข้อมูลได้: {fund_code} (HTTP {response.status_code}, ลองครั้งที่ {attempts + 1})")
                time.sleep(retry_delay)
        except Exception as e:
            print(f"เกิดข้อผิดพลาดกับ {fund_code}: {e} (ลองครั้งที่ {attempts + 1})")
            time.sleep(retry_delay)
            
    print(f"❌ โหลดข้อมูลไม่สำเร็จ: {fund_code} หลังจากลอง {max_retries} ครั้ง")
    return None

def run_concurrent_fetching(all_list, endpoint_suffix, file_name, max_workers=20):
    """จัดการการทำงานแบบขนานและบันทึกผลลัพธ์เป็น CSV เดียว"""
    start_time = time.time()
    data_list = []
    
    print(f"▶️ เริ่มดึงข้อมูล {file_name} แบบขนาน ({len(all_list)} รายการ)...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # ใช้ list comprehension เพื่อส่งคำขอพร้อมกัน
        future_to_code = {
            executor.submit(
                fetch_data_concurrently, 
                code, 
                endpoint_suffix, 
                globals().get('max_retries', 3), 
                globals().get('retry_delay', 2)
            ): code 
            for code in all_list
        }
        
        # รวบรวมผลลัพธ์เมื่อการทำงานเสร็จสิ้น
        for future in concurrent.futures.as_completed(future_to_code):
            data = future.result()
            if data is not None:
                data_list.append(data)

    if data_list:
        df = pd.json_normalize(data_list)
        if not df.empty:
            file_path = f'{local_path}{file_name}'
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"บันทึกไฟล์ {file_name} เรียบร้อยแล้ว ✅ (เวลาที่ใช้: {elapsed_time:.2f} วินาที)")
        else:
            print(f"⚠️ ไม่พบข้อมูลที่ดึงได้สำหรับ {file_name}")
    else:
        print(f"❌ ไม่สามารถดึงข้อมูลใดๆ ได้เลยสำหรับ {file_name}")

# overview
run_concurrent_fetching(all_list, "", 'overview.csv', max_workers=20)

# performance
run_concurrent_fetching(all_list, '/performance', 'performance.csv', max_workers=20)

# portfolio
run_concurrent_fetching(all_list, '/portfolio', 'portfolio.csv', max_workers=20)

# fee
run_concurrent_fetching(all_list, '/fee', 'fee.csv', max_workers=20)

# NAV
# -------------------------Indicator------------------------------------------
# ฟังก์ชันคำนวน MACD
def compute_macd(df, fast=12, slow=26, signal=9):
    # คำนวณ EMA
    ema_fast = df['value'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['value'].ewm(span=slow, adjust=False).mean()

    # MACD Line
    df['MACD'] = ema_fast - ema_slow

    # Signal Line (EMA ของ MACD)
    df['MACD_Signal'] = df['MACD'].ewm(span=signal, adjust=False).mean()

    # Histogram (MACD - Signal)
    df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']

    return df

# ฟังก์ชันคำนวน RSI
def compute_rsi(series, period=14):
    delta = series.diff()

    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi

# ฟังก์ชันคำนวณ Stochastic Oscillator
# ใช้ Close อย่างเดียว (Pseudo Stochastic) เนื่องจากไม่มี High Low
def compute_stochastic(df, k_period=14, d_period=3):
    try:
        if 'value' not in df.columns:
            raise ValueError("Missing required column: 'value'")

        low_min = df['value'].rolling(window=k_period, min_periods=1).min()
        high_max = df['value'].rolling(window=k_period, min_periods=1).max()

        denominator = high_max - low_min
        percent_k = np.where(denominator == 0, np.nan, 100 * (df['value'] - low_min) / denominator)

        df['%K'] = pd.Series(percent_k, index=df.index)
        df['%D'] = df['%K'].rolling(window=d_period, min_periods=1).mean()

        return df

    except Exception as e:
        print(f"Stochastic error: {e}")
        df['%K'] = np.nan
        df['%D'] = np.nan
        return df

# ฟังก์ชันคำนวณ Bollinger Bands
def compute_bollinger_bands(df, window=20, num_std=2):
    try:
        if 'value' not in df.columns:
            raise ValueError("Missing required column: 'value'")

        # คำนวณค่าเฉลี่ยและส่วนเบี่ยงเบนมาตรฐานแบบ rolling
        rolling_mean = df['value'].rolling(window=window, min_periods=1).mean()
        rolling_std = df['value'].rolling(window=window, min_periods=1).std()

        # สร้าง Bollinger Bands
        df['Bollinger_Mid'] = rolling_mean
        df['Bollinger_Upper'] = rolling_mean + (rolling_std * num_std)
        df['Bollinger_Lower'] = rolling_mean - (rolling_std * num_std)

        return df

    except Exception as e:
        print(f"Bollinger Bands error: {e}")
        df['Bollinger_Mid'] = np.nan
        df['Bollinger_Upper'] = np.nan
        df['Bollinger_Lower'] = np.nan
        return df
    

def compute_above_ema_streak(df, ema_col='EMA20', close_col='value', result_col=None):
    if result_col is None:
        result_col = f'Above_{ema_col}_Streak'

    streak = []
    count = 0
    in_streak = None  # None = ยังไม่เริ่ม, True = อยู่ฝั่งเหนือ EMA, False = อยู่ฝั่งต่ำกว่า EMA

    for i in range(len(df)):
        close = df[close_col].iloc[i]
        ema = df[ema_col].iloc[i]

        if pd.notna(close) and pd.notna(ema):
            if close > ema:  # เหนือ EMA
                if in_streak is True:
                    count += 1
                else:
                    count = 1
                in_streak = True
            else:  # ต่ำกว่า EMA
                if in_streak is False:
                    count -= 1
                else:
                    count = -1
                in_streak = False
        else:
            count, in_streak = 0, None

        streak.append(count)

    df[result_col] = streak
    return df

def compute_macd_with_streaks(df, value_col='value', fast=12, slow=26, signal=9):
    # คำนวณ MACD Line
    ema_fast = df[value_col].ewm(span=fast, adjust=False).mean()
    ema_slow = df[value_col].ewm(span=slow, adjust=False).mean()
    df['MACD'] = ema_fast - ema_slow

    # Signal Line (EMA ของ MACD)
    df['MACD_Signal'] = df['MACD'].ewm(span=signal, adjust=False).mean()

    # Histogram
    df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']

    # --- Streak: MACD > Signal ---
    streak, count, in_streak = [], 0, None
    for macd, signal_line in zip(df['MACD'], df['MACD_Signal']):
        if pd.notna(macd) and pd.notna(signal_line):
            if macd > signal_line:  # ฝั่งบวก
                if in_streak is True:
                    count += 1
                else:
                    count = 1
                in_streak = True
            else:  # ฝั่งลบ
                if in_streak is False:
                    count -= 1
                else:
                    count = -1
                in_streak = False
        else:
            count, in_streak = 0, None
        streak.append(count)
    df['MACD_Bull_Streak'] = streak

    # --- Streak: Histogram > 0 ---
    hist_streak, count, in_streak = [], 0, None
    for hist in df['MACD_Histogram']:
        if pd.notna(hist):
            if hist > 0:  # ฝั่งบวก
                if in_streak is True:
                    count += 1
                else:
                    count = 1
                in_streak = True
            else:  # ฝั่งลบ
                if in_streak is False:
                    count -= 1
                else:
                    count = -1
                in_streak = False
        else:
            count, in_streak = 0, None
        hist_streak.append(count)
    df['MACD_Hist_Streak'] = hist_streak

    return df

# -------------------------------------------------------------------------

def fetch_and_save_nav(fund_code, local_path_nav, max_retries, retry_delay):
    """
    ดึงข้อมูล NAV, คำนวณ Indicator และบันทึกเป็น CSV แยกไฟล์
    """
    url = f"https://www.finnomena.com/fn3/api/fund/v2/public/funds/{fund_code}/nav/q?range=MAX"
    
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.finnomena.com/fund/list',
    }
    
    for attempts in range(max_retries):
        try:
            # 1. ดึงข้อมูล
            response = requests.get(url, headers=headers, timeout=30, verify=False)
            
            # การจัดการ Rate Limit (HTTP 429)
            if response.status_code == 429:
                print(f"Too many requests (429) for {fund_code}. รอ 300s ก่อนลองใหม่...")
                time.sleep(300)
                continue
            
            response.raise_for_status()
            data = response.json()
            
            if 'data' not in data or 'navs' not in data['data'] or not data['data']['navs']:
                print(f"⚠️ ไม่พบข้อมูล NAV สำหรับ {fund_code}")
                return fund_code
            
            # 2. แยกข้อมูล NAV List และ Metadata
            metadata = data['data']
            navs_list = metadata['navs']
            
            # 3. Normalize NAV List
            df_nav_records = pd.json_normalize(navs_list)
            
            # 4. แปลงวันที่และจัดเรียง (สำคัญสำหรับการคำนวณ Time Series)
            df_nav_records['date'] = pd.to_datetime(df_nav_records['date'])
            df_nav_records = df_nav_records.sort_values('date').reset_index(drop=True)
            
            # -------------------- START INDICATOR CALCULATION --------------------
            
            if 'value' in df_nav_records.columns:
                
                # a. คำนวณ % Change
                df_nav_records['pct_change_value'] = df_nav_records['value'].pct_change().round(4)
                if 'amount' in df_nav_records.columns:
                    df_nav_records['pct_change_amount'] = df_nav_records['amount'].pct_change().round(4)
                
                # b. คำนวณ EMAs (พื้นฐาน)
                # คำนวณ ewm5, ewm20, ewm50, ewm90, ewm200
                # df_nav_records['EMA5'] = df_nav_records['value'].ewm(span=5, adjust=False).mean()
                df_nav_records['EMA20'] = df_nav_records['value'].ewm(span=20, adjust=False).mean()
                df_nav_records['EMA50'] = df_nav_records['value'].ewm(span=50, adjust=False).mean()
                df_nav_records['EMA90'] = df_nav_records['value'].ewm(span=90, adjust=False).mean()
                df_nav_records['EMA200'] = df_nav_records['value'].ewm(span=200, adjust=False).mean()

                # c. คำนวน MACD
                df_nav_records = compute_macd(df_nav_records)

                # d. คำนวน RSI
                df_nav_records['RSI14'] = compute_rsi(df_nav_records['value'], period=14)
                df_nav_records['RSI_Signal'] = df_nav_records['RSI14'].ewm(span=5, adjust=False).mean() # แบบ EMA
                # df_nav_records['RSI_Signal'] = df_nav_records['RSI14'].rolling(windows=5).mean() # แบบ SMA

                # e. คำนวน Stochastic Oscillator
                df_nav_records = compute_stochastic(df_nav_records, k_period=14, d_period=3)

                # f. คำนวณ Bollinger Bands
                df_nav_records = compute_bollinger_bands(df_nav_records, window=20, num_std=2)

                # g. นับวัน Close > EMA อย่างต่อเนื่อง (Streaks)
                # df_nav_records = compute_above_ema_streak(df_nav_records, ema_col='EMA5', result_col='Above_EMA5_Streak')
                df_nav_records = compute_above_ema_streak(df_nav_records, ema_col='EMA20', result_col='Above_EMA20_Streak')
                df_nav_records = compute_above_ema_streak(df_nav_records, ema_col='EMA50', result_col='Above_EMA50_Streak')
                df_nav_records = compute_above_ema_streak(df_nav_records, ema_col='EMA90', result_col='Above_EMA90_Streak')
                df_nav_records = compute_above_ema_streak(df_nav_records, ema_col='EMA200', result_col='Above_EMA200_Streak')

                # h. นับวัน MACD Streak
                df_nav_records = compute_macd_with_streaks(df_nav_records)
            
            # -------------------- END INDICATOR CALCULATION --------------------

            # --- Compare Scale ---

            # Normalization (Min–Max Scaling) [0, 1]
            df_nav_records['norm_value'] = (df_nav_records['value'] - df_nav_records['value'].min()) / (df_nav_records['value'].max() - df_nav_records['value'].min())
            # Standardization (Z-score) ค่าเฉลี่ย 0 และส่วนเบี่ยงเบนมาตรฐาน 1
            df_nav_records['z_value'] = (df_nav_records['value'] - df_nav_records['value'].mean()) / df_nav_records['value'].std()
            # Rebase = 0 (หรือ 0.0) ณ วันเริ่มต้น วิธีเปรียบเทียบให้จุดเริ่มต้นเริ่มที่ 0
            df_nav_records['rebased'] = (df_nav_records['value'] / df_nav_records['value'].iloc[0] - 1) * 100

            # วันที่เป็น datetime และตั้งเป็น Index หรือสกัดปีออกมา
            df_nav_records['year'] = df_nav_records['date'].dt.year
            # Rebase = 0 รายปี (เริ่มคำนวณใหม่ทุกวันที่ 1 ของปีนั้นๆ)
            df_nav_records['rebased_yearly'] = df_nav_records.groupby('year')['value'].transform(lambda x: (x / x.iloc[0] - 1) * 100)

            # --- สิ้นสุด Compare Scale ---

            # 5. เพิ่มคอลัมน์ Metadata และ Max Date Flag
            df_nav_records['fund_id'] = metadata.get('fund_id') # เปลี่ยนชื่อคอลัมน์เพื่อไม่ให้ซ้ำซ้อน
            df_nav_records['short_code'] = metadata.get('short_code')
            df_nav_records['status'] = data.get('status')
            df_nav_records['service_code'] = data.get('service_code')
            # df_nav_records['fund_id'] = fund_code
            
            # Max Date Flag
            df_nav_records['max_date_per_fund'] = df_nav_records['date'].max()
            df_nav_records['is_max_date'] = (df_nav_records['date'] == df_nav_records['max_date_per_fund'])
            
            # แปลงวันที่กลับเป็น String Format เดิมก่อนบันทึก
            df_nav_records['date'] = df_nav_records['date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            df_nav_records['max_date_per_fund'] = df_nav_records['max_date_per_fund'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            # 6. บันทึกไฟล์ NAV
            file_name = f'{fund_code}_nav.csv'
            file_path = os.path.join(local_path_nav, file_name)
            
            df_nav_records.to_csv(file_path, index=False, encoding='utf-8-sig') 
            print(f"Saved {file_path} (NAV records: {len(df_nav_records)} rows)")
            
            return fund_code

        except requests.exceptions.HTTPError as http_err:
            current_delay = retry_delay * (2 ** attempts)
            print(f"HTTP Error for {fund_code}: {http_err}. Waiting {current_delay} วินาทีก่อนลองใหม่...")
            time.sleep(current_delay)
        except Exception as e:
            current_delay = retry_delay * (2 ** attempts)
            print(f"เกิดข้อผิดพลาดกับ {fund_code}: {e} (ลองครั้งที่ {attempts + 1}, รอ {current_delay}s)")
            time.sleep(current_delay)

    print(f"❌ โหลดข้อมูล NAV ไม่สำเร็จ: {fund_code} หลังจากลอง {max_retries} ครั้ง")
    return None


def run_concurrent_nav_fetching(all_list, local_path_nav, max_workers=20):
    """จัดการการทำงานแบบขนานเพื่อดึงและบันทึก NAV"""
    start_time = time.time()
    
    max_retries = globals().get('max_retries', 3)
    retry_delay = globals().get('retry_delay', 5)
    
    print(f"▶️ เริ่มดึงข้อมูล NAV แบบขนาน ({len(all_list)} รายการ)...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {
            executor.submit(
                fetch_and_save_nav, 
                code, 
                local_path_nav, 
                max_retries, 
                retry_delay
            ): code 
            for code in all_list
        }
        
        # ติดตามความคืบหน้า (การบันทึกไฟล์เกิดขึ้นใน fetch_and_save_nav แล้ว)
        for future in concurrent.futures.as_completed(future_to_code):
            pass

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"✅ การดึงข้อมูล NAV เสร็จสิ้น (เวลาที่ใช้: {elapsed_time:.2f} วินาที)")

# รันฟังก์ชันนี้
run_concurrent_nav_fetching(all_list, local_path_nav, max_workers=20)

# merged for optimize = performance + overview
# อ่านไฟล์ทั้งสอง
file_path1 = f'{local_path}performance.csv'
file_path2 = f'{local_path}overview.csv'

df_perf = pd.read_csv(file_path1)
df_over = pd.read_csv(file_path2)

# แสดงตัวอย่างชื่อคอลัมน์เพื่อตรวจสอบ
print("Performance Columns:", df_perf.columns)
print("Overview Columns:", df_over.columns)

# เลือกเฉพาะคอลัมน์ที่ต้องการจาก overview
selected_columns = [
    'data.short_code',
    'data.fund_id',
    'data.name_en',
    'data.name_th',
    'data.risk_spectrum',
    'data.tax_saving_fund',
    'data.dividend_policy',
    'data.aimc_category_name_en',
    'data.aimc_broad_category_name_en',
    'data.amc_name_en',
    'data.risk_level',
    'data.fund_tax_type'
]

df_over_selected = df_over[selected_columns]

# Join โดยใช้ data.short_code เป็นคีย์
df_merged = pd.merge(df_perf, df_over_selected, on='data.short_code', how='inner')

# ✅ ลบคำว่า 'data.' ออกจากชื่อคอลัมน์ที่มี
df_merged.columns = [col.replace('data.', '') for col in df_merged.columns]

# แสดงผลลัพธ์บางส่วน
print(df_merged.head())

# (ถ้าต้องการบันทึกไฟล์)
df_merged.to_csv(f'{local_path}merged_fund_for_optimzed.csv', index=False, encoding="utf-8-sig")

# --------------------------------------------------------
# Optimize A
## filter file for optimized
df_filtered = []

# อ่านไฟล์ merged
file_path = f'{local_path}merged_fund_for_optimzed.csv'
df_perf = pd.read_csv(file_path)

# ✅ กรองข้อมูลตามเงื่อนไข
df_filtered = df_perf[
    (df_perf['amc_name_en'] == 'KASSET') &
    # (df_perf['dividend_policy'] == 'ไม่จ่าย') &
    (df_perf['fund_tax_type'].isna()) &
    (~df_perf['short_code'].str.contains('PVD', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('สถาบัน', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('ห้ามขายผู้ลงทุนรายย่อย', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('เพื่อผู้ลงทุนนิติบุคคล', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('อัตโน', case=False, na=False))
]

# แสดงผลลัพธ์บางส่วน
print(df_filtered.head())

# แสดงจำนวนกองทุนทั้งหมดที่ตรงเงื่อนไข
print(f"✅ จำนวนกองทุนที่ตรงเงื่อนไขทั้งหมด: {len(df_filtered)}")

## Optimize to file
from scipy.optimize import minimize
from sklearn.utils import resample

# 🔹 เปลี่ยนจากอ่านไฟล์เป็นใช้ df_filtered แทน
df = df_filtered.copy()

# 📌 ตั้งชื่อ column และเลือก field ช่วงเวลามาคำนวน
col_fund = "short_code" # เปลี่ยนชื่อแล้วที่ด้านบน "data.short_code"
col_return = "total_return_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.total_return_1y"
col_std = "std_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.std_1y"

# 📌 ล้างค่าความเสี่ยงต่ำผิดปกติ
df = df[[col_fund, col_return, col_std]].dropna()
df[col_std] = df[col_std].clip(lower=1e-6)

portfolios = []

# ------------------------
# 📌 ฟังก์ชัน optimization (maximize Sharpe = minimize -Sharpe)
def neg_sharpe(weights, returns, risks):
    port_return = np.dot(weights, returns)
    port_std = np.sqrt(np.dot(weights**2, risks**2))  # simplified std
    return -port_return / port_std

# ------------------------
# 📌 สร้าง 10000 แผนพอร์ต (10 กองทุนต่อแผน)
for i in range(10000): # <<<<< ปรับจำนวนแผน
    sample = resample(df, n_samples=10, replace=False, random_state=i) # <<<<< ปรับจำนวนกอง
    codes = sample[col_fund].values
    returns = sample[col_return].values
    risks = sample[col_std].values

    w0 = np.ones(10) / 10 # <<<<< ปรับจำนวนกอง
    bounds = [(0, 0.3)] * 10 # <<<<< ปรับจำนวนกอง, กำหนดขั้นต่ำและเพดานสูงสุดต่อกอง
    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

    result = minimize(neg_sharpe, w0, args=(returns, risks), method='SLSQP',
                      bounds=bounds, constraints=constraints)

    if result.success:
        weights = result.x
        df_out = pd.DataFrame({
            'fund_code': codes,
            'weight': weights,
            'expected_return': returns,
            'risk_std': risks
        })
        df_out['plan'] = f'Plan {i+1}'
        df_out['sharpe_ratio'] = df_out['expected_return'] / df_out['risk_std']
        portfolios.append(df_out)
    else:
        print(f"❌ Optimization failed for Plan {i+1}")

# ------------------------
# 📌 รวมและบันทึก
if portfolios:
    final_df = pd.concat(portfolios)
    final_df.to_csv(f'{local_path_opt}optimized_portfolios_plan_A.csv', index=False, encoding="utf-8-sig")
    print(f"✅ บันทึกไฟล์: optimized_portfolios_plan_A.csv")
else:
    print("❌ ไม่มีพอร์ตที่ optimize สำเร็จ")

## top frequency ranking
# 1. คำนวณหาค่าเฉลี่ยของแต่ละแผน (Plan Summary)
plan_summary = final_df.groupby('plan').agg({
    'sharpe_ratio': 'mean', # หรือจะใช้พอร์ต Sharpe ที่คำนวณจากก้อนรวมก็ได้
    'expected_return': 'sum', # ผลตอบแทนคาดการณ์ของพอร์ต (ถ้าถ่วงน้ำหนักแล้ว)
}).reset_index()

# 2. คัดเลือกเฉพาะ Top 1% ของแผนที่ดีที่สุด (เช่น 100 แผนจาก 10,000)
top_plans_threshold = plan_summary['sharpe_ratio'].quantile(0.99)
top_plans = plan_summary[plan_summary['sharpe_ratio'] >= top_plans_threshold]['plan']

# 3. ดูว่าในแผนระดับ Top เหล่านี้ มีกองทุนไหนปรากฏตัวบ่อยที่สุด
top_funds_analysis = final_df[final_df['plan'].isin(top_plans)]

# นับจำนวนครั้งที่ถูกรับเลือก และน้ำหนักเฉลี่ยที่ถูกใส่ในพอร์ต
fund_stats = top_funds_analysis.groupby('fund_code').agg({
    'weight': ['count', 'mean', 'sum'],
    'expected_return': 'first',
    'risk_std': 'first'
}).reset_index()

# จัดรูปแบบตารางให้ดูง่าย
fund_stats.columns = ['fund_code', 'appearances', 'avg_weight', 'total_weight_impact', 'return', 'risk']
fund_stats = fund_stats.sort_values(by='appearances', ascending=False)

print("⭐ กองทุนที่ปรากฏในแผนระดับ Top 1% บ่อยที่สุด:")
print(fund_stats.head(10))

# บันทึกผลออกมาดู
fund_stats.to_csv(f'{local_path_opt}top_frequency_ranking_A.csv', index=False, encoding="utf-8-sig")

## graph ef + export ef
#Plot กราฟข้อมูล optimized_portfolios + csv
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# 🔹 โหลดไฟล์ข้อมูลแผนพอร์ตที่ optimize แล้ว
file_path = f'{local_path_opt}optimized_portfolios_plan_A.csv'
df = pd.read_csv(file_path)

# 🔹 คำนวณ Risk / Return / Sharpe Ratio ของแต่ละพอร์ต
port_summary = []
for plan, group in df.groupby("plan"):
    weights = group['weight'].values
    returns = group['expected_return'].values
    risks = group['risk_std'].values

    port_return = np.dot(weights, returns)
    port_risk = np.sqrt(np.dot(weights**2, risks**2))
    port_sharpe = port_return / port_risk if port_risk > 0 else np.nan

    port_summary.append({
        'plan': plan,
        'return': port_return,
        'risk': port_risk,
        'sharpe': port_sharpe
    })

# 🔹 แปลงเป็น DataFrame และกรองค่าที่ใช้ log ไม่ได้
ef_df = pd.DataFrame(port_summary)
ef_df = ef_df[(ef_df['risk'] > 0) & (ef_df['return'] > 0)]

# 🔹 คัดจุดบนเส้น Efficient Frontier: "return สูงสุดต่อระดับ risk"
ef_sorted = ef_df.sort_values(by='risk')
efficient_points = []
max_return = -np.inf
for _, row in ef_sorted.iterrows():
    if row['return'] > max_return:
        efficient_points.append(row)
        max_return = row['return']
efficient_points_df = pd.DataFrame(efficient_points)

# 🔹 Export จุด Efficient Frontier ไปใช้ต่อใน Power BI
efficient_points_df.to_csv(f'{local_path_opt}efficient_frontier_points_A.csv', index=False, encoding="utf-8-sig")

# 🔹 วาดกราฟ
plt.figure(figsize=(10, 6))

# จุดทั้งหมด
sc = plt.scatter(ef_df['risk'], ef_df['return'], c=ef_df['sharpe'], cmap='viridis', s=5)

# เส้น Efficient Frontier
plt.plot(efficient_points_df['risk'], efficient_points_df['return'], 'r--', linewidth=2, label='Efficient Frontier')

# ------------------plot log scale------------------------
# ปรับแกน log scale
plt.xscale('log')
plt.yscale('log')

# แสดงตัวเลขเป็นทศนิยม (ไม่ใช้ exponential)
formatter = ScalarFormatter()
formatter.set_scientific(False)
formatter.set_useOffset(False)
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

# อื่น ๆ
plt.colorbar(sc, label='Sharpe Ratio')
plt.xlabel('Portfolio Risk (Std) [Log Scale]')
plt.ylabel('Portfolio Expected Return [Log Scale]')
plt.title(f'Efficient Frontier of Optimized Plans A (Log-Log Scale)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.tight_layout()
# plt.show()
# --------------------------------------------------------

## correlation coefficient
import pandas as pd
import numpy as np
import ast
import seaborn as sns
import matplotlib
matplotlib.use('Agg') # บังคับโหมดรันเบื้องหลัง (ไม่มีหน้าจอ)
import matplotlib.pyplot as plt

# 1. โหลดข้อมูล
df = df_filtered.copy()

# 2. ฟังก์ชันสำหรับดึงข้อมูล Yearly Returns ออกมาเป็นชุดตัวเลข
def extract_yearly_returns(json_str):
    try:
        data_list = ast.literal_eval(json_str)
        # ดึงเฉพาะค่า value (return) ออกมาเป็น list
        return [item['value'] for item in data_list]
    except:
        return None

# 3. เตรียมข้อมูล (ตัวอย่าง: คัดมาเฉพาะกองที่มี Sharpe Ratio สูงสุด 500 อันดับแรกเพื่อประมวลผล)
top_funds = df.nlargest(500, 'sharpe_ratio_1y')[['short_code', 'returns_year']]

# 4. สร้างตารางใหม่ที่ Column คือชื่อกองทุน และ Row คือผลตอบแทนแต่ละปี
returns_dict = {}
for idx, row in top_funds.iterrows():
    y_returns = extract_yearly_returns(row['returns_year'])
    if y_returns:
        returns_dict[row['short_code']] = y_returns

df_corr_input = pd.DataFrame(returns_dict)

# 5. คำนวณ Correlation
correlation_matrix = df_corr_input.corr()

# 6. บันทึกเป็น CSV
correlation_matrix.to_csv(f'{local_path_opt}correlation_coefficient_A.csv', encoding='utf-8-sig')

# 7. พล็อต Heatmap
plt.figure(figsize=(24, 20))

sns.heatmap(
    correlation_matrix, 
    annot=True,            # แสดงตัวเลข
    cmap='RdYlGn',         # สีแดง-เหลือง-เขียว
    fmt=".2f",             # ทศนิยม 2 ตำแหน่ง
    annot_kws={"size": 6}, # ** ปรับขนาดตัวเลขในช่องให้เล็กลงพอดีกับช่อง เพื่อให้อ่านง่าย **
    linewidths=0.0,        # เพิ่มเส้นคั่นบางๆ ให้แยกช่องชัดเจน
    cbar_kws={"label": "Correlation Coefficient"} # ใส่ชื่อบอกว่าแถบขวาคืออะไร
)

plt.title(f'Correlation Matrix of Funds A (Based on 10-Year Annual Returns)', fontsize=20, pad=20)
plt.xticks(rotation=90, fontsize=9) # หมุนชื่อกองทุนแนวตั้ง
plt.yticks(rotation=0, fontsize=9)  # ชื่อกองทุนแนวนอน
plt.tight_layout()

# แสดงผล
# plt.show()

# --------------------------------------------------------
# Optimize B
## filter file for optimized
df_filtered = []

# อ่านไฟล์ merged
file_path = f'{local_path}merged_fund_for_optimzed.csv'
df_perf = pd.read_csv(file_path)

# ✅ กรองข้อมูลตามเงื่อนไข
df_filtered = df_perf[
    (df_perf['amc_name_en'] == 'SCBAM') &
    # (df_perf['dividend_policy'] == 'ไม่จ่าย') &
    (df_perf['fund_tax_type'].isna()) &
    (~df_perf['short_code'].str.contains('PVD', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('สถาบัน', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('ห้ามขายผู้ลงทุนรายย่อย', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('เพื่อผู้ลงทุนนิติบุคคล', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('อัตโน', case=False, na=False))
]

# แสดงผลลัพธ์บางส่วน
print(df_filtered.head())

# แสดงจำนวนกองทุนทั้งหมดที่ตรงเงื่อนไข
print(f"✅ จำนวนกองทุนที่ตรงเงื่อนไขทั้งหมด: {len(df_filtered)}")

## Optimize to file
from scipy.optimize import minimize
from sklearn.utils import resample

# 🔹 เปลี่ยนจากอ่านไฟล์เป็นใช้ df_filtered แทน
df = df_filtered.copy()

# 📌 ตั้งชื่อ column และเลือก field ช่วงเวลามาคำนวน
col_fund = "short_code" # เปลี่ยนชื่อแล้วที่ด้านบน "data.short_code"
col_return = "total_return_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.total_return_1y"
col_std = "std_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.std_1y"

# 📌 ล้างค่าความเสี่ยงต่ำผิดปกติ
df = df[[col_fund, col_return, col_std]].dropna()
df[col_std] = df[col_std].clip(lower=1e-6)

portfolios = []

# ------------------------
# 📌 ฟังก์ชัน optimization (maximize Sharpe = minimize -Sharpe)
def neg_sharpe(weights, returns, risks):
    port_return = np.dot(weights, returns)
    port_std = np.sqrt(np.dot(weights**2, risks**2))  # simplified std
    return -port_return / port_std

# ------------------------
# 📌 สร้าง 10000 แผนพอร์ต (10 กองทุนต่อแผน)
for i in range(10000): # <<<<< ปรับจำนวนแผน
    sample = resample(df, n_samples=10, replace=False, random_state=i) # <<<<< ปรับจำนวนกอง
    codes = sample[col_fund].values
    returns = sample[col_return].values
    risks = sample[col_std].values

    w0 = np.ones(10) / 10 # <<<<< ปรับจำนวนกอง
    bounds = [(0, 0.3)] * 10 # <<<<< ปรับจำนวนกอง, กำหนดขั้นต่ำและเพดานสูงสุดต่อกอง
    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

    result = minimize(neg_sharpe, w0, args=(returns, risks), method='SLSQP',
                      bounds=bounds, constraints=constraints)

    if result.success:
        weights = result.x
        df_out = pd.DataFrame({
            'fund_code': codes,
            'weight': weights,
            'expected_return': returns,
            'risk_std': risks
        })
        df_out['plan'] = f'Plan {i+1}'
        df_out['sharpe_ratio'] = df_out['expected_return'] / df_out['risk_std']
        portfolios.append(df_out)
    else:
        print(f"❌ Optimization failed for Plan {i+1}")

# ------------------------
# 📌 รวมและบันทึก
if portfolios:
    final_df = pd.concat(portfolios)
    final_df.to_csv(f'{local_path_opt}optimized_portfolios_plan_B.csv', index=False, encoding="utf-8-sig")
    print(f"✅ บันทึกไฟล์: optimized_portfolios_plan_B.csv")
else:
    print("❌ ไม่มีพอร์ตที่ optimize สำเร็จ")

## top frequency ranking
# 1. คำนวณหาค่าเฉลี่ยของแต่ละแผน (Plan Summary)
plan_summary = final_df.groupby('plan').agg({
    'sharpe_ratio': 'mean', # หรือจะใช้พอร์ต Sharpe ที่คำนวณจากก้อนรวมก็ได้
    'expected_return': 'sum', # ผลตอบแทนคาดการณ์ของพอร์ต (ถ้าถ่วงน้ำหนักแล้ว)
}).reset_index()

# 2. คัดเลือกเฉพาะ Top 1% ของแผนที่ดีที่สุด (เช่น 100 แผนจาก 10,000)
top_plans_threshold = plan_summary['sharpe_ratio'].quantile(0.99)
top_plans = plan_summary[plan_summary['sharpe_ratio'] >= top_plans_threshold]['plan']

# 3. ดูว่าในแผนระดับ Top เหล่านี้ มีกองทุนไหนปรากฏตัวบ่อยที่สุด
top_funds_analysis = final_df[final_df['plan'].isin(top_plans)]

# นับจำนวนครั้งที่ถูกรับเลือก และน้ำหนักเฉลี่ยที่ถูกใส่ในพอร์ต
fund_stats = top_funds_analysis.groupby('fund_code').agg({
    'weight': ['count', 'mean', 'sum'],
    'expected_return': 'first',
    'risk_std': 'first'
}).reset_index()

# จัดรูปแบบตารางให้ดูง่าย
fund_stats.columns = ['fund_code', 'appearances', 'avg_weight', 'total_weight_impact', 'return', 'risk']
fund_stats = fund_stats.sort_values(by='appearances', ascending=False)

print("⭐ กองทุนที่ปรากฏในแผนระดับ Top 1% บ่อยที่สุด:")
print(fund_stats.head(10))

# บันทึกผลออกมาดู
fund_stats.to_csv(f'{local_path_opt}top_frequency_ranking_B.csv', index=False, encoding="utf-8-sig")

## graph ef + export ef
#Plot กราฟข้อมูล optimized_portfolios + csv
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# 🔹 โหลดไฟล์ข้อมูลแผนพอร์ตที่ optimize แล้ว
file_path = f'{local_path_opt}optimized_portfolios_plan_B.csv'
df = pd.read_csv(file_path)

# 🔹 คำนวณ Risk / Return / Sharpe Ratio ของแต่ละพอร์ต
port_summary = []
for plan, group in df.groupby("plan"):
    weights = group['weight'].values
    returns = group['expected_return'].values
    risks = group['risk_std'].values

    port_return = np.dot(weights, returns)
    port_risk = np.sqrt(np.dot(weights**2, risks**2))
    port_sharpe = port_return / port_risk if port_risk > 0 else np.nan

    port_summary.append({
        'plan': plan,
        'return': port_return,
        'risk': port_risk,
        'sharpe': port_sharpe
    })

# 🔹 แปลงเป็น DataFrame และกรองค่าที่ใช้ log ไม่ได้
ef_df = pd.DataFrame(port_summary)
ef_df = ef_df[(ef_df['risk'] > 0) & (ef_df['return'] > 0)]

# 🔹 คัดจุดบนเส้น Efficient Frontier: "return สูงสุดต่อระดับ risk"
ef_sorted = ef_df.sort_values(by='risk')
efficient_points = []
max_return = -np.inf
for _, row in ef_sorted.iterrows():
    if row['return'] > max_return:
        efficient_points.append(row)
        max_return = row['return']
efficient_points_df = pd.DataFrame(efficient_points)

# 🔹 Export จุด Efficient Frontier ไปใช้ต่อใน Power BI
efficient_points_df.to_csv(f'{local_path_opt}efficient_frontier_points_B.csv', index=False, encoding="utf-8-sig")

# 🔹 วาดกราฟ
plt.figure(figsize=(10, 6))

# จุดทั้งหมด
sc = plt.scatter(ef_df['risk'], ef_df['return'], c=ef_df['sharpe'], cmap='viridis', s=5)

# เส้น Efficient Frontier
plt.plot(efficient_points_df['risk'], efficient_points_df['return'], 'r--', linewidth=2, label='Efficient Frontier')

# ------------------plot log scale------------------------
# ปรับแกน log scale
plt.xscale('log')
plt.yscale('log')

# แสดงตัวเลขเป็นทศนิยม (ไม่ใช้ exponential)
formatter = ScalarFormatter()
formatter.set_scientific(False)
formatter.set_useOffset(False)
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

# อื่น ๆ
plt.colorbar(sc, label='Sharpe Ratio')
plt.xlabel('Portfolio Risk (Std) [Log Scale]')
plt.ylabel('Portfolio Expected Return [Log Scale]')
plt.title(f'Efficient Frontier of Optimized Plans B (Log-Log Scale)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.tight_layout()
# plt.show()
# --------------------------------------------------------

## correlation coefficient
import pandas as pd
import numpy as np
import ast
import seaborn as sns
import matplotlib
matplotlib.use('Agg') # บังคับโหมดรันเบื้องหลัง (ไม่มีหน้าจอ)
import matplotlib.pyplot as plt

# 1. โหลดข้อมูล
df = df_filtered.copy()

# 2. ฟังก์ชันสำหรับดึงข้อมูล Yearly Returns ออกมาเป็นชุดตัวเลข
def extract_yearly_returns(json_str):
    try:
        data_list = ast.literal_eval(json_str)
        # ดึงเฉพาะค่า value (return) ออกมาเป็น list
        return [item['value'] for item in data_list]
    except:
        return None

# 3. เตรียมข้อมูล (ตัวอย่าง: คัดมาเฉพาะกองที่มี Sharpe Ratio สูงสุด 500 อันดับแรกเพื่อประมวลผล)
top_funds = df.nlargest(500, 'sharpe_ratio_1y')[['short_code', 'returns_year']]

# 4. สร้างตารางใหม่ที่ Column คือชื่อกองทุน และ Row คือผลตอบแทนแต่ละปี
returns_dict = {}
for idx, row in top_funds.iterrows():
    y_returns = extract_yearly_returns(row['returns_year'])
    if y_returns:
        returns_dict[row['short_code']] = y_returns

df_corr_input = pd.DataFrame(returns_dict)

# 5. คำนวณ Correlation
correlation_matrix = df_corr_input.corr()

# 6. บันทึกเป็น CSV
correlation_matrix.to_csv(f'{local_path_opt}correlation_coefficient_B.csv', encoding='utf-8-sig')

# 7. พล็อต Heatmap
plt.figure(figsize=(24, 20))

sns.heatmap(
    correlation_matrix, 
    annot=True,            # แสดงตัวเลข
    cmap='RdYlGn',         # สีแดง-เหลือง-เขียว
    fmt=".2f",             # ทศนิยม 2 ตำแหน่ง
    annot_kws={"size": 6}, # ** ปรับขนาดตัวเลขในช่องให้เล็กลงพอดีกับช่อง เพื่อให้อ่านง่าย **
    linewidths=0.0,        # เพิ่มเส้นคั่นบางๆ ให้แยกช่องชัดเจน
    cbar_kws={"label": "Correlation Coefficient"} # ใส่ชื่อบอกว่าแถบขวาคืออะไร
)

plt.title(f'Correlation Matrix of Funds B (Based on 10-Year Annual Returns)', fontsize=20, pad=20)
plt.xticks(rotation=90, fontsize=9) # หมุนชื่อกองทุนแนวตั้ง
plt.yticks(rotation=0, fontsize=9)  # ชื่อกองทุนแนวนอน
plt.tight_layout()

# แสดงผล
# plt.show()

# --------------------------------------------------------
# Optimize C
## filter file for optimized
df_filtered = []

# อ่านไฟล์ merged
file_path = f'{local_path}merged_fund_for_optimzed.csv'
df_perf = pd.read_csv(file_path)

# ✅ กรองข้อมูลตามเงื่อนไข
df_filtered = df_perf[
    (df_perf['amc_name_en'] == 'KTAM') &
    # (df_perf['dividend_policy'] == 'ไม่จ่าย') &
    (df_perf['fund_tax_type'].isna()) &
    (~df_perf['short_code'].str.contains('PVD', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('สถาบัน', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('ห้ามขายผู้ลงทุนรายย่อย', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('เพื่อผู้ลงทุนนิติบุคคล', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('อัตโน', case=False, na=False))
]

# แสดงผลลัพธ์บางส่วน
print(df_filtered.head())

# แสดงจำนวนกองทุนทั้งหมดที่ตรงเงื่อนไข
print(f"✅ จำนวนกองทุนที่ตรงเงื่อนไขทั้งหมด: {len(df_filtered)}")

## Optimize to file
from scipy.optimize import minimize
from sklearn.utils import resample

# 🔹 เปลี่ยนจากอ่านไฟล์เป็นใช้ df_filtered แทน
df = df_filtered.copy()

# 📌 ตั้งชื่อ column และเลือก field ช่วงเวลามาคำนวน
col_fund = "short_code" # เปลี่ยนชื่อแล้วที่ด้านบน "data.short_code"
col_return = "total_return_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.total_return_1y"
col_std = "std_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.std_1y"

# 📌 ล้างค่าความเสี่ยงต่ำผิดปกติ
df = df[[col_fund, col_return, col_std]].dropna()
df[col_std] = df[col_std].clip(lower=1e-6)

portfolios = []

# ------------------------
# 📌 ฟังก์ชัน optimization (maximize Sharpe = minimize -Sharpe)
def neg_sharpe(weights, returns, risks):
    port_return = np.dot(weights, returns)
    port_std = np.sqrt(np.dot(weights**2, risks**2))  # simplified std
    return -port_return / port_std

# ------------------------
# 📌 สร้าง 10000 แผนพอร์ต (10 กองทุนต่อแผน)
for i in range(10000): # <<<<< ปรับจำนวนแผน
    sample = resample(df, n_samples=10, replace=False, random_state=i) # <<<<< ปรับจำนวนกอง
    codes = sample[col_fund].values
    returns = sample[col_return].values
    risks = sample[col_std].values

    w0 = np.ones(10) / 10 # <<<<< ปรับจำนวนกอง
    bounds = [(0, 0.3)] * 10 # <<<<< ปรับจำนวนกอง, กำหนดขั้นต่ำและเพดานสูงสุดต่อกอง
    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

    result = minimize(neg_sharpe, w0, args=(returns, risks), method='SLSQP',
                      bounds=bounds, constraints=constraints)

    if result.success:
        weights = result.x
        df_out = pd.DataFrame({
            'fund_code': codes,
            'weight': weights,
            'expected_return': returns,
            'risk_std': risks
        })
        df_out['plan'] = f'Plan {i+1}'
        df_out['sharpe_ratio'] = df_out['expected_return'] / df_out['risk_std']
        portfolios.append(df_out)
    else:
        print(f"❌ Optimization failed for Plan {i+1}")

# ------------------------
# 📌 รวมและบันทึก
if portfolios:
    final_df = pd.concat(portfolios)
    final_df.to_csv(f'{local_path_opt}optimized_portfolios_plan_C.csv', index=False, encoding="utf-8-sig")
    print(f"✅ บันทึกไฟล์: optimized_portfolios_plan_C.csv")
else:
    print("❌ ไม่มีพอร์ตที่ optimize สำเร็จ")

## top frequency ranking
# 1. คำนวณหาค่าเฉลี่ยของแต่ละแผน (Plan Summary)
plan_summary = final_df.groupby('plan').agg({
    'sharpe_ratio': 'mean', # หรือจะใช้พอร์ต Sharpe ที่คำนวณจากก้อนรวมก็ได้
    'expected_return': 'sum', # ผลตอบแทนคาดการณ์ของพอร์ต (ถ้าถ่วงน้ำหนักแล้ว)
}).reset_index()

# 2. คัดเลือกเฉพาะ Top 1% ของแผนที่ดีที่สุด (เช่น 100 แผนจาก 10,000)
top_plans_threshold = plan_summary['sharpe_ratio'].quantile(0.99)
top_plans = plan_summary[plan_summary['sharpe_ratio'] >= top_plans_threshold]['plan']

# 3. ดูว่าในแผนระดับ Top เหล่านี้ มีกองทุนไหนปรากฏตัวบ่อยที่สุด
top_funds_analysis = final_df[final_df['plan'].isin(top_plans)]

# นับจำนวนครั้งที่ถูกรับเลือก และน้ำหนักเฉลี่ยที่ถูกใส่ในพอร์ต
fund_stats = top_funds_analysis.groupby('fund_code').agg({
    'weight': ['count', 'mean', 'sum'],
    'expected_return': 'first',
    'risk_std': 'first'
}).reset_index()

# จัดรูปแบบตารางให้ดูง่าย
fund_stats.columns = ['fund_code', 'appearances', 'avg_weight', 'total_weight_impact', 'return', 'risk']
fund_stats = fund_stats.sort_values(by='appearances', ascending=False)

print("⭐ กองทุนที่ปรากฏในแผนระดับ Top 1% บ่อยที่สุด:")
print(fund_stats.head(10))

# บันทึกผลออกมาดู
fund_stats.to_csv(f'{local_path_opt}top_frequency_ranking_C.csv', index=False, encoding="utf-8-sig")

## graph ef + export ef
#Plot กราฟข้อมูล optimized_portfolios + csv
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# 🔹 โหลดไฟล์ข้อมูลแผนพอร์ตที่ optimize แล้ว
file_path = f'{local_path_opt}optimized_portfolios_plan_C.csv'
df = pd.read_csv(file_path)

# 🔹 คำนวณ Risk / Return / Sharpe Ratio ของแต่ละพอร์ต
port_summary = []
for plan, group in df.groupby("plan"):
    weights = group['weight'].values
    returns = group['expected_return'].values
    risks = group['risk_std'].values

    port_return = np.dot(weights, returns)
    port_risk = np.sqrt(np.dot(weights**2, risks**2))
    port_sharpe = port_return / port_risk if port_risk > 0 else np.nan

    port_summary.append({
        'plan': plan,
        'return': port_return,
        'risk': port_risk,
        'sharpe': port_sharpe
    })

# 🔹 แปลงเป็น DataFrame และกรองค่าที่ใช้ log ไม่ได้
ef_df = pd.DataFrame(port_summary)
ef_df = ef_df[(ef_df['risk'] > 0) & (ef_df['return'] > 0)]

# 🔹 คัดจุดบนเส้น Efficient Frontier: "return สูงสุดต่อระดับ risk"
ef_sorted = ef_df.sort_values(by='risk')
efficient_points = []
max_return = -np.inf
for _, row in ef_sorted.iterrows():
    if row['return'] > max_return:
        efficient_points.append(row)
        max_return = row['return']
efficient_points_df = pd.DataFrame(efficient_points)

# 🔹 Export จุด Efficient Frontier ไปใช้ต่อใน Power BI
efficient_points_df.to_csv(f'{local_path_opt}efficient_frontier_points_C.csv', index=False, encoding="utf-8-sig")

# 🔹 วาดกราฟ
plt.figure(figsize=(10, 6))

# จุดทั้งหมด
sc = plt.scatter(ef_df['risk'], ef_df['return'], c=ef_df['sharpe'], cmap='viridis', s=5)

# เส้น Efficient Frontier
plt.plot(efficient_points_df['risk'], efficient_points_df['return'], 'r--', linewidth=2, label='Efficient Frontier')

# ------------------plot log scale------------------------
# ปรับแกน log scale
plt.xscale('log')
plt.yscale('log')

# แสดงตัวเลขเป็นทศนิยม (ไม่ใช้ exponential)
formatter = ScalarFormatter()
formatter.set_scientific(False)
formatter.set_useOffset(False)
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

# อื่น ๆ
plt.colorbar(sc, label='Sharpe Ratio')
plt.xlabel('Portfolio Risk (Std) [Log Scale]')
plt.ylabel('Portfolio Expected Return [Log Scale]')
plt.title(f'Efficient Frontier of Optimized Plans C (Log-Log Scale)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.tight_layout()
# plt.show()
# --------------------------------------------------------

## correlation coefficient
import pandas as pd
import numpy as np
import ast
import seaborn as sns
import matplotlib
matplotlib.use('Agg') # บังคับโหมดรันเบื้องหลัง (ไม่มีหน้าจอ)
import matplotlib.pyplot as plt

# 1. โหลดข้อมูล
df = df_filtered.copy()

# 2. ฟังก์ชันสำหรับดึงข้อมูล Yearly Returns ออกมาเป็นชุดตัวเลข
def extract_yearly_returns(json_str):
    try:
        data_list = ast.literal_eval(json_str)
        # ดึงเฉพาะค่า value (return) ออกมาเป็น list
        return [item['value'] for item in data_list]
    except:
        return None

# 3. เตรียมข้อมูล (ตัวอย่าง: คัดมาเฉพาะกองที่มี Sharpe Ratio สูงสุด 500 อันดับแรกเพื่อประมวลผล)
top_funds = df.nlargest(500, 'sharpe_ratio_1y')[['short_code', 'returns_year']]

# 4. สร้างตารางใหม่ที่ Column คือชื่อกองทุน และ Row คือผลตอบแทนแต่ละปี
returns_dict = {}
for idx, row in top_funds.iterrows():
    y_returns = extract_yearly_returns(row['returns_year'])
    if y_returns:
        returns_dict[row['short_code']] = y_returns

df_corr_input = pd.DataFrame(returns_dict)

# 5. คำนวณ Correlation
correlation_matrix = df_corr_input.corr()

# 6. บันทึกเป็น CSV
correlation_matrix.to_csv(f'{local_path_opt}correlation_coefficient_C.csv', encoding='utf-8-sig')

# 7. พล็อต Heatmap
plt.figure(figsize=(24, 20))

sns.heatmap(
    correlation_matrix, 
    annot=True,            # แสดงตัวเลข
    cmap='RdYlGn',         # สีแดง-เหลือง-เขียว
    fmt=".2f",             # ทศนิยม 2 ตำแหน่ง
    annot_kws={"size": 6}, # ** ปรับขนาดตัวเลขในช่องให้เล็กลงพอดีกับช่อง เพื่อให้อ่านง่าย **
    linewidths=0.0,        # เพิ่มเส้นคั่นบางๆ ให้แยกช่องชัดเจน
    cbar_kws={"label": "Correlation Coefficient"} # ใส่ชื่อบอกว่าแถบขวาคืออะไร
)

plt.title(f'Correlation Matrix of Funds C (Based on 10-Year Annual Returns)', fontsize=20, pad=20)
plt.xticks(rotation=90, fontsize=9) # หมุนชื่อกองทุนแนวตั้ง
plt.yticks(rotation=0, fontsize=9)  # ชื่อกองทุนแนวนอน
plt.tight_layout()

# แสดงผล
# plt.show()

# --------------------------------------------------------
# Optimize D
## filter file for optimized
df_filtered = []

# อ่านไฟล์ merged
file_path = f'{local_path}merged_fund_for_optimzed.csv'
df_perf = pd.read_csv(file_path)

# ✅ กรองข้อมูลตามเงื่อนไข
df_filtered = df_perf[
    (df_perf['amc_name_en'] == 'KSAM') &
    # (df_perf['dividend_policy'] == 'ไม่จ่าย') &
    (df_perf['fund_tax_type'].isna()) &
    (~df_perf['short_code'].str.contains('PVD', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('สถาบัน', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('ห้ามขายผู้ลงทุนรายย่อย', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('เพื่อผู้ลงทุนนิติบุคคล', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('อัตโน', case=False, na=False))
]

# แสดงผลลัพธ์บางส่วน
print(df_filtered.head())

# แสดงจำนวนกองทุนทั้งหมดที่ตรงเงื่อนไข
print(f"✅ จำนวนกองทุนที่ตรงเงื่อนไขทั้งหมด: {len(df_filtered)}")

## Optimize to file
from scipy.optimize import minimize
from sklearn.utils import resample

# 🔹 เปลี่ยนจากอ่านไฟล์เป็นใช้ df_filtered แทน
df = df_filtered.copy()

# 📌 ตั้งชื่อ column และเลือก field ช่วงเวลามาคำนวน
col_fund = "short_code" # เปลี่ยนชื่อแล้วที่ด้านบน "data.short_code"
col_return = "total_return_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.total_return_1y"
col_std = "std_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.std_1y"

# 📌 ล้างค่าความเสี่ยงต่ำผิดปกติ
df = df[[col_fund, col_return, col_std]].dropna()
df[col_std] = df[col_std].clip(lower=1e-6)

portfolios = []

# ------------------------
# 📌 ฟังก์ชัน optimization (maximize Sharpe = minimize -Sharpe)
def neg_sharpe(weights, returns, risks):
    port_return = np.dot(weights, returns)
    port_std = np.sqrt(np.dot(weights**2, risks**2))  # simplified std
    return -port_return / port_std

# ------------------------
# 📌 สร้าง 10000 แผนพอร์ต (10 กองทุนต่อแผน)
for i in range(10000): # <<<<< ปรับจำนวนแผน
    sample = resample(df, n_samples=10, replace=False, random_state=i) # <<<<< ปรับจำนวนกอง
    codes = sample[col_fund].values
    returns = sample[col_return].values
    risks = sample[col_std].values

    w0 = np.ones(10) / 10 # <<<<< ปรับจำนวนกอง
    bounds = [(0, 0.3)] * 10 # <<<<< ปรับจำนวนกอง, กำหนดขั้นต่ำและเพดานสูงสุดต่อกอง
    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

    result = minimize(neg_sharpe, w0, args=(returns, risks), method='SLSQP',
                      bounds=bounds, constraints=constraints)

    if result.success:
        weights = result.x
        df_out = pd.DataFrame({
            'fund_code': codes,
            'weight': weights,
            'expected_return': returns,
            'risk_std': risks
        })
        df_out['plan'] = f'Plan {i+1}'
        df_out['sharpe_ratio'] = df_out['expected_return'] / df_out['risk_std']
        portfolios.append(df_out)
    else:
        print(f"❌ Optimization failed for Plan {i+1}")

# ------------------------
# 📌 รวมและบันทึก
if portfolios:
    final_df = pd.concat(portfolios)
    final_df.to_csv(f'{local_path_opt}optimized_portfolios_plan_D.csv', index=False, encoding="utf-8-sig")
    print(f"✅ บันทึกไฟล์: optimized_portfolios_plan_D.csv")
else:
    print("❌ ไม่มีพอร์ตที่ optimize สำเร็จ")

## top frequency ranking
# 1. คำนวณหาค่าเฉลี่ยของแต่ละแผน (Plan Summary)
plan_summary = final_df.groupby('plan').agg({
    'sharpe_ratio': 'mean', # หรือจะใช้พอร์ต Sharpe ที่คำนวณจากก้อนรวมก็ได้
    'expected_return': 'sum', # ผลตอบแทนคาดการณ์ของพอร์ต (ถ้าถ่วงน้ำหนักแล้ว)
}).reset_index()

# 2. คัดเลือกเฉพาะ Top 1% ของแผนที่ดีที่สุด (เช่น 100 แผนจาก 10,000)
top_plans_threshold = plan_summary['sharpe_ratio'].quantile(0.99)
top_plans = plan_summary[plan_summary['sharpe_ratio'] >= top_plans_threshold]['plan']

# 3. ดูว่าในแผนระดับ Top เหล่านี้ มีกองทุนไหนปรากฏตัวบ่อยที่สุด
top_funds_analysis = final_df[final_df['plan'].isin(top_plans)]

# นับจำนวนครั้งที่ถูกรับเลือก และน้ำหนักเฉลี่ยที่ถูกใส่ในพอร์ต
fund_stats = top_funds_analysis.groupby('fund_code').agg({
    'weight': ['count', 'mean', 'sum'],
    'expected_return': 'first',
    'risk_std': 'first'
}).reset_index()

# จัดรูปแบบตารางให้ดูง่าย
fund_stats.columns = ['fund_code', 'appearances', 'avg_weight', 'total_weight_impact', 'return', 'risk']
fund_stats = fund_stats.sort_values(by='appearances', ascending=False)

print("⭐ กองทุนที่ปรากฏในแผนระดับ Top 1% บ่อยที่สุด:")
print(fund_stats.head(10))

# บันทึกผลออกมาดู
fund_stats.to_csv(f'{local_path_opt}top_frequency_ranking_D.csv', index=False, encoding="utf-8-sig")

## graph ef + export ef
#Plot กราฟข้อมูล optimized_portfolios + csv
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# 🔹 โหลดไฟล์ข้อมูลแผนพอร์ตที่ optimize แล้ว
file_path = f'{local_path_opt}optimized_portfolios_plan_D.csv'
df = pd.read_csv(file_path)

# 🔹 คำนวณ Risk / Return / Sharpe Ratio ของแต่ละพอร์ต
port_summary = []
for plan, group in df.groupby("plan"):
    weights = group['weight'].values
    returns = group['expected_return'].values
    risks = group['risk_std'].values

    port_return = np.dot(weights, returns)
    port_risk = np.sqrt(np.dot(weights**2, risks**2))
    port_sharpe = port_return / port_risk if port_risk > 0 else np.nan

    port_summary.append({
        'plan': plan,
        'return': port_return,
        'risk': port_risk,
        'sharpe': port_sharpe
    })

# 🔹 แปลงเป็น DataFrame และกรองค่าที่ใช้ log ไม่ได้
ef_df = pd.DataFrame(port_summary)
ef_df = ef_df[(ef_df['risk'] > 0) & (ef_df['return'] > 0)]

# 🔹 คัดจุดบนเส้น Efficient Frontier: "return สูงสุดต่อระดับ risk"
ef_sorted = ef_df.sort_values(by='risk')
efficient_points = []
max_return = -np.inf
for _, row in ef_sorted.iterrows():
    if row['return'] > max_return:
        efficient_points.append(row)
        max_return = row['return']
efficient_points_df = pd.DataFrame(efficient_points)

# 🔹 Export จุด Efficient Frontier ไปใช้ต่อใน Power BI
efficient_points_df.to_csv(f'{local_path_opt}efficient_frontier_points_D.csv', index=False, encoding="utf-8-sig")

# 🔹 วาดกราฟ
plt.figure(figsize=(10, 6))

# จุดทั้งหมด
sc = plt.scatter(ef_df['risk'], ef_df['return'], c=ef_df['sharpe'], cmap='viridis', s=5)

# เส้น Efficient Frontier
plt.plot(efficient_points_df['risk'], efficient_points_df['return'], 'r--', linewidth=2, label='Efficient Frontier')

# ------------------plot log scale------------------------
# ปรับแกน log scale
plt.xscale('log')
plt.yscale('log')

# แสดงตัวเลขเป็นทศนิยม (ไม่ใช้ exponential)
formatter = ScalarFormatter()
formatter.set_scientific(False)
formatter.set_useOffset(False)
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

# อื่น ๆ
plt.colorbar(sc, label='Sharpe Ratio')
plt.xlabel('Portfolio Risk (Std) [Log Scale]')
plt.ylabel('Portfolio Expected Return [Log Scale]')
plt.title(f'Efficient Frontier of Optimized Plans D (Log-Log Scale)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.tight_layout()
# plt.show()
# --------------------------------------------------------

## correlation coefficient
import pandas as pd
import numpy as np
import ast
import seaborn as sns
import matplotlib
matplotlib.use('Agg') # บังคับโหมดรันเบื้องหลัง (ไม่มีหน้าจอ)
import matplotlib.pyplot as plt

# 1. โหลดข้อมูล
df = df_filtered.copy()

# 2. ฟังก์ชันสำหรับดึงข้อมูล Yearly Returns ออกมาเป็นชุดตัวเลข
def extract_yearly_returns(json_str):
    try:
        data_list = ast.literal_eval(json_str)
        # ดึงเฉพาะค่า value (return) ออกมาเป็น list
        return [item['value'] for item in data_list]
    except:
        return None

# 3. เตรียมข้อมูล (ตัวอย่าง: คัดมาเฉพาะกองที่มี Sharpe Ratio สูงสุด 500 อันดับแรกเพื่อประมวลผล)
top_funds = df.nlargest(500, 'sharpe_ratio_1y')[['short_code', 'returns_year']]

# 4. สร้างตารางใหม่ที่ Column คือชื่อกองทุน และ Row คือผลตอบแทนแต่ละปี
returns_dict = {}
for idx, row in top_funds.iterrows():
    y_returns = extract_yearly_returns(row['returns_year'])
    if y_returns:
        returns_dict[row['short_code']] = y_returns

df_corr_input = pd.DataFrame(returns_dict)

# 5. คำนวณ Correlation
correlation_matrix = df_corr_input.corr()

# 6. บันทึกเป็น CSV
correlation_matrix.to_csv(f'{local_path_opt}correlation_coefficient_D.csv', encoding='utf-8-sig')

# 7. พล็อต Heatmap
plt.figure(figsize=(24, 20))

sns.heatmap(
    correlation_matrix, 
    annot=True,            # แสดงตัวเลข
    cmap='RdYlGn',         # สีแดง-เหลือง-เขียว
    fmt=".2f",             # ทศนิยม 2 ตำแหน่ง
    annot_kws={"size": 6}, # ** ปรับขนาดตัวเลขในช่องให้เล็กลงพอดีกับช่อง เพื่อให้อ่านง่าย **
    linewidths=0.0,        # เพิ่มเส้นคั่นบางๆ ให้แยกช่องชัดเจน
    cbar_kws={"label": "Correlation Coefficient"} # ใส่ชื่อบอกว่าแถบขวาคืออะไร
)

plt.title(f'Correlation Matrix of Funds D (Based on 10-Year Annual Returns)', fontsize=20, pad=20)
plt.xticks(rotation=90, fontsize=9) # หมุนชื่อกองทุนแนวตั้ง
plt.yticks(rotation=0, fontsize=9)  # ชื่อกองทุนแนวนอน
plt.tight_layout()

# แสดงผล
# plt.show()

# --------------------------------------------------------
# Optimize E
## filter file for optimized
df_filtered = []

# อ่านไฟล์ merged
file_path = f'{local_path}merged_fund_for_optimzed.csv'
df_perf = pd.read_csv(file_path)

# ✅ กรองข้อมูลตามเงื่อนไข
df_filtered = df_perf[
    (df_perf['amc_name_en'] == 'TISCOAM') &
    # (df_perf['dividend_policy'] == 'ไม่จ่าย') &
    (df_perf['fund_tax_type'].isna()) &
    (~df_perf['short_code'].str.contains('PVD', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('สถาบัน', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('ห้ามขายผู้ลงทุนรายย่อย', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('เพื่อผู้ลงทุนนิติบุคคล', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('อัตโน', case=False, na=False))
]

# แสดงผลลัพธ์บางส่วน
print(df_filtered.head())

# แสดงจำนวนกองทุนทั้งหมดที่ตรงเงื่อนไข
print(f"✅ จำนวนกองทุนที่ตรงเงื่อนไขทั้งหมด: {len(df_filtered)}")

## Optimize to file
from scipy.optimize import minimize
from sklearn.utils import resample

# 🔹 เปลี่ยนจากอ่านไฟล์เป็นใช้ df_filtered แทน
df = df_filtered.copy()

# 📌 ตั้งชื่อ column และเลือก field ช่วงเวลามาคำนวน
col_fund = "short_code" # เปลี่ยนชื่อแล้วที่ด้านบน "data.short_code"
col_return = "total_return_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.total_return_1y"
col_std = "std_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.std_1y"

# 📌 ล้างค่าความเสี่ยงต่ำผิดปกติ
df = df[[col_fund, col_return, col_std]].dropna()
df[col_std] = df[col_std].clip(lower=1e-6)

portfolios = []

# ------------------------
# 📌 ฟังก์ชัน optimization (maximize Sharpe = minimize -Sharpe)
def neg_sharpe(weights, returns, risks):
    port_return = np.dot(weights, returns)
    port_std = np.sqrt(np.dot(weights**2, risks**2))  # simplified std
    return -port_return / port_std

# ------------------------
# 📌 สร้าง 10000 แผนพอร์ต (10 กองทุนต่อแผน)
for i in range(10000): # <<<<< ปรับจำนวนแผน
    sample = resample(df, n_samples=10, replace=False, random_state=i) # <<<<< ปรับจำนวนกอง
    codes = sample[col_fund].values
    returns = sample[col_return].values
    risks = sample[col_std].values

    w0 = np.ones(10) / 10 # <<<<< ปรับจำนวนกอง
    bounds = [(0, 0.3)] * 10 # <<<<< ปรับจำนวนกอง, กำหนดขั้นต่ำและเพดานสูงสุดต่อกอง
    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

    result = minimize(neg_sharpe, w0, args=(returns, risks), method='SLSQP',
                      bounds=bounds, constraints=constraints)

    if result.success:
        weights = result.x
        df_out = pd.DataFrame({
            'fund_code': codes,
            'weight': weights,
            'expected_return': returns,
            'risk_std': risks
        })
        df_out['plan'] = f'Plan {i+1}'
        df_out['sharpe_ratio'] = df_out['expected_return'] / df_out['risk_std']
        portfolios.append(df_out)
    else:
        print(f"❌ Optimization failed for Plan {i+1}")

# ------------------------
# 📌 รวมและบันทึก
if portfolios:
    final_df = pd.concat(portfolios)
    final_df.to_csv(f'{local_path_opt}optimized_portfolios_plan_E.csv', index=False, encoding="utf-8-sig")
    print(f"✅ บันทึกไฟล์: optimized_portfolios_plan_E.csv")
else:
    print("❌ ไม่มีพอร์ตที่ optimize สำเร็จ")

## top frequency ranking
# 1. คำนวณหาค่าเฉลี่ยของแต่ละแผน (Plan Summary)
plan_summary = final_df.groupby('plan').agg({
    'sharpe_ratio': 'mean', # หรือจะใช้พอร์ต Sharpe ที่คำนวณจากก้อนรวมก็ได้
    'expected_return': 'sum', # ผลตอบแทนคาดการณ์ของพอร์ต (ถ้าถ่วงน้ำหนักแล้ว)
}).reset_index()

# 2. คัดเลือกเฉพาะ Top 1% ของแผนที่ดีที่สุด (เช่น 100 แผนจาก 10,000)
top_plans_threshold = plan_summary['sharpe_ratio'].quantile(0.99)
top_plans = plan_summary[plan_summary['sharpe_ratio'] >= top_plans_threshold]['plan']

# 3. ดูว่าในแผนระดับ Top เหล่านี้ มีกองทุนไหนปรากฏตัวบ่อยที่สุด
top_funds_analysis = final_df[final_df['plan'].isin(top_plans)]

# นับจำนวนครั้งที่ถูกรับเลือก และน้ำหนักเฉลี่ยที่ถูกใส่ในพอร์ต
fund_stats = top_funds_analysis.groupby('fund_code').agg({
    'weight': ['count', 'mean', 'sum'],
    'expected_return': 'first',
    'risk_std': 'first'
}).reset_index()

# จัดรูปแบบตารางให้ดูง่าย
fund_stats.columns = ['fund_code', 'appearances', 'avg_weight', 'total_weight_impact', 'return', 'risk']
fund_stats = fund_stats.sort_values(by='appearances', ascending=False)

print("⭐ กองทุนที่ปรากฏในแผนระดับ Top 1% บ่อยที่สุด:")
print(fund_stats.head(10))

# บันทึกผลออกมาดู
fund_stats.to_csv(f'{local_path_opt}top_frequency_ranking_E.csv', index=False, encoding="utf-8-sig")

## graph ef + export ef
#Plot กราฟข้อมูล optimized_portfolios + csv
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# 🔹 โหลดไฟล์ข้อมูลแผนพอร์ตที่ optimize แล้ว
file_path = f'{local_path_opt}optimized_portfolios_plan_E.csv'
df = pd.read_csv(file_path)

# 🔹 คำนวณ Risk / Return / Sharpe Ratio ของแต่ละพอร์ต
port_summary = []
for plan, group in df.groupby("plan"):
    weights = group['weight'].values
    returns = group['expected_return'].values
    risks = group['risk_std'].values

    port_return = np.dot(weights, returns)
    port_risk = np.sqrt(np.dot(weights**2, risks**2))
    port_sharpe = port_return / port_risk if port_risk > 0 else np.nan

    port_summary.append({
        'plan': plan,
        'return': port_return,
        'risk': port_risk,
        'sharpe': port_sharpe
    })

# 🔹 แปลงเป็น DataFrame และกรองค่าที่ใช้ log ไม่ได้
ef_df = pd.DataFrame(port_summary)
ef_df = ef_df[(ef_df['risk'] > 0) & (ef_df['return'] > 0)]

# 🔹 คัดจุดบนเส้น Efficient Frontier: "return สูงสุดต่อระดับ risk"
ef_sorted = ef_df.sort_values(by='risk')
efficient_points = []
max_return = -np.inf
for _, row in ef_sorted.iterrows():
    if row['return'] > max_return:
        efficient_points.append(row)
        max_return = row['return']
efficient_points_df = pd.DataFrame(efficient_points)

# 🔹 Export จุด Efficient Frontier ไปใช้ต่อใน Power BI
efficient_points_df.to_csv(f'{local_path_opt}efficient_frontier_points_E.csv', index=False, encoding="utf-8-sig")

# 🔹 วาดกราฟ
plt.figure(figsize=(10, 6))

# จุดทั้งหมด
sc = plt.scatter(ef_df['risk'], ef_df['return'], c=ef_df['sharpe'], cmap='viridis', s=5)

# เส้น Efficient Frontier
plt.plot(efficient_points_df['risk'], efficient_points_df['return'], 'r--', linewidth=2, label='Efficient Frontier')

# ------------------plot log scale------------------------
# ปรับแกน log scale
plt.xscale('log')
plt.yscale('log')

# แสดงตัวเลขเป็นทศนิยม (ไม่ใช้ exponential)
formatter = ScalarFormatter()
formatter.set_scientific(False)
formatter.set_useOffset(False)
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

# อื่น ๆ
plt.colorbar(sc, label='Sharpe Ratio')
plt.xlabel('Portfolio Risk (Std) [Log Scale]')
plt.ylabel('Portfolio Expected Return [Log Scale]')
plt.title(f'Efficient Frontier of Optimized Plans E (Log-Log Scale)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.tight_layout()
# plt.show()
# --------------------------------------------------------

## correlation coefficient
import pandas as pd
import numpy as np
import ast
import seaborn as sns
import matplotlib
matplotlib.use('Agg') # บังคับโหมดรันเบื้องหลัง (ไม่มีหน้าจอ)
import matplotlib.pyplot as plt

# 1. โหลดข้อมูล
df = df_filtered.copy()

# 2. ฟังก์ชันสำหรับดึงข้อมูล Yearly Returns ออกมาเป็นชุดตัวเลข
def extract_yearly_returns(json_str):
    try:
        data_list = ast.literal_eval(json_str)
        # ดึงเฉพาะค่า value (return) ออกมาเป็น list
        return [item['value'] for item in data_list]
    except:
        return None

# 3. เตรียมข้อมูล (ตัวอย่าง: คัดมาเฉพาะกองที่มี Sharpe Ratio สูงสุด 500 อันดับแรกเพื่อประมวลผล)
top_funds = df.nlargest(500, 'sharpe_ratio_1y')[['short_code', 'returns_year']]

# 4. สร้างตารางใหม่ที่ Column คือชื่อกองทุน และ Row คือผลตอบแทนแต่ละปี
returns_dict = {}
for idx, row in top_funds.iterrows():
    y_returns = extract_yearly_returns(row['returns_year'])
    if y_returns:
        returns_dict[row['short_code']] = y_returns

df_corr_input = pd.DataFrame(returns_dict)

# 5. คำนวณ Correlation
correlation_matrix = df_corr_input.corr()

# 6. บันทึกเป็น CSV
correlation_matrix.to_csv(f'{local_path_opt}correlation_coefficient_E.csv', encoding='utf-8-sig')

# 7. พล็อต Heatmap
plt.figure(figsize=(24, 20))

sns.heatmap(
    correlation_matrix, 
    annot=True,            # แสดงตัวเลข
    cmap='RdYlGn',         # สีแดง-เหลือง-เขียว
    fmt=".2f",             # ทศนิยม 2 ตำแหน่ง
    annot_kws={"size": 6}, # ** ปรับขนาดตัวเลขในช่องให้เล็กลงพอดีกับช่อง เพื่อให้อ่านง่าย **
    linewidths=0.0,        # เพิ่มเส้นคั่นบางๆ ให้แยกช่องชัดเจน
    cbar_kws={"label": "Correlation Coefficient"} # ใส่ชื่อบอกว่าแถบขวาคืออะไร
)

plt.title(f'Correlation Matrix of Funds E (Based on 10-Year Annual Returns)', fontsize=20, pad=20)
plt.xticks(rotation=90, fontsize=9) # หมุนชื่อกองทุนแนวตั้ง
plt.yticks(rotation=0, fontsize=9)  # ชื่อกองทุนแนวนอน
plt.tight_layout()

# แสดงผล
# plt.show()

# --------------------------------------------------------
# Optimize F
## filter file for optimized
df_filtered = []

# อ่านไฟล์ merged
file_path = f'{local_path}merged_fund_for_optimzed.csv'
df_perf = pd.read_csv(file_path)

# ✅ กรองข้อมูลตามเงื่อนไข
df_filtered = df_perf[
    (df_perf['amc_name_en'] == 'EASTSPRING') &
    # (df_perf['dividend_policy'] == 'ไม่จ่าย') &
    (df_perf['fund_tax_type'].isna()) &
    (~df_perf['short_code'].str.contains('PVD', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('สถาบัน', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('ห้ามขายผู้ลงทุนรายย่อย', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('เพื่อผู้ลงทุนนิติบุคคล', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('อัตโน', case=False, na=False))
]

# แสดงผลลัพธ์บางส่วน
print(df_filtered.head())

# แสดงจำนวนกองทุนทั้งหมดที่ตรงเงื่อนไข
print(f"✅ จำนวนกองทุนที่ตรงเงื่อนไขทั้งหมด: {len(df_filtered)}")

## Optimize to file
from scipy.optimize import minimize
from sklearn.utils import resample

# 🔹 เปลี่ยนจากอ่านไฟล์เป็นใช้ df_filtered แทน
df = df_filtered.copy()

# 📌 ตั้งชื่อ column และเลือก field ช่วงเวลามาคำนวน
col_fund = "short_code" # เปลี่ยนชื่อแล้วที่ด้านบน "data.short_code"
col_return = "total_return_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.total_return_1y"
col_std = "std_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.std_1y"

# 📌 ล้างค่าความเสี่ยงต่ำผิดปกติ
df = df[[col_fund, col_return, col_std]].dropna()
df[col_std] = df[col_std].clip(lower=1e-6)

portfolios = []

# ------------------------
# 📌 ฟังก์ชัน optimization (maximize Sharpe = minimize -Sharpe)
def neg_sharpe(weights, returns, risks):
    port_return = np.dot(weights, returns)
    port_std = np.sqrt(np.dot(weights**2, risks**2))  # simplified std
    return -port_return / port_std

# ------------------------
# 📌 สร้าง 10000 แผนพอร์ต (10 กองทุนต่อแผน)
for i in range(10000): # <<<<< ปรับจำนวนแผน
    sample = resample(df, n_samples=10, replace=False, random_state=i) # <<<<< ปรับจำนวนกอง
    codes = sample[col_fund].values
    returns = sample[col_return].values
    risks = sample[col_std].values

    w0 = np.ones(10) / 10 # <<<<< ปรับจำนวนกอง
    bounds = [(0, 0.3)] * 10 # <<<<< ปรับจำนวนกอง, กำหนดขั้นต่ำและเพดานสูงสุดต่อกอง
    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

    result = minimize(neg_sharpe, w0, args=(returns, risks), method='SLSQP',
                      bounds=bounds, constraints=constraints)

    if result.success:
        weights = result.x
        df_out = pd.DataFrame({
            'fund_code': codes,
            'weight': weights,
            'expected_return': returns,
            'risk_std': risks
        })
        df_out['plan'] = f'Plan {i+1}'
        df_out['sharpe_ratio'] = df_out['expected_return'] / df_out['risk_std']
        portfolios.append(df_out)
    else:
        print(f"❌ Optimization failed for Plan {i+1}")

# ------------------------
# 📌 รวมและบันทึก
if portfolios:
    final_df = pd.concat(portfolios)
    final_df.to_csv(f'{local_path_opt}optimized_portfolios_plan_F.csv', index=False, encoding="utf-8-sig")
    print(f"✅ บันทึกไฟล์: optimized_portfolios_plan_F.csv")
else:
    print("❌ ไม่มีพอร์ตที่ optimize สำเร็จ")

## top frequency ranking
# 1. คำนวณหาค่าเฉลี่ยของแต่ละแผน (Plan Summary)
plan_summary = final_df.groupby('plan').agg({
    'sharpe_ratio': 'mean', # หรือจะใช้พอร์ต Sharpe ที่คำนวณจากก้อนรวมก็ได้
    'expected_return': 'sum', # ผลตอบแทนคาดการณ์ของพอร์ต (ถ้าถ่วงน้ำหนักแล้ว)
}).reset_index()

# 2. คัดเลือกเฉพาะ Top 1% ของแผนที่ดีที่สุด (เช่น 100 แผนจาก 10,000)
top_plans_threshold = plan_summary['sharpe_ratio'].quantile(0.99)
top_plans = plan_summary[plan_summary['sharpe_ratio'] >= top_plans_threshold]['plan']

# 3. ดูว่าในแผนระดับ Top เหล่านี้ มีกองทุนไหนปรากฏตัวบ่อยที่สุด
top_funds_analysis = final_df[final_df['plan'].isin(top_plans)]

# นับจำนวนครั้งที่ถูกรับเลือก และน้ำหนักเฉลี่ยที่ถูกใส่ในพอร์ต
fund_stats = top_funds_analysis.groupby('fund_code').agg({
    'weight': ['count', 'mean', 'sum'],
    'expected_return': 'first',
    'risk_std': 'first'
}).reset_index()

# จัดรูปแบบตารางให้ดูง่าย
fund_stats.columns = ['fund_code', 'appearances', 'avg_weight', 'total_weight_impact', 'return', 'risk']
fund_stats = fund_stats.sort_values(by='appearances', ascending=False)

print("⭐ กองทุนที่ปรากฏในแผนระดับ Top 1% บ่อยที่สุด:")
print(fund_stats.head(10))

# บันทึกผลออกมาดู
fund_stats.to_csv(f'{local_path_opt}top_frequency_ranking_F.csv', index=False, encoding="utf-8-sig")

## graph ef + export ef
#Plot กราฟข้อมูล optimized_portfolios + csv
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# 🔹 โหลดไฟล์ข้อมูลแผนพอร์ตที่ optimize แล้ว
file_path = f'{local_path_opt}optimized_portfolios_plan_F.csv'
df = pd.read_csv(file_path)

# 🔹 คำนวณ Risk / Return / Sharpe Ratio ของแต่ละพอร์ต
port_summary = []
for plan, group in df.groupby("plan"):
    weights = group['weight'].values
    returns = group['expected_return'].values
    risks = group['risk_std'].values

    port_return = np.dot(weights, returns)
    port_risk = np.sqrt(np.dot(weights**2, risks**2))
    port_sharpe = port_return / port_risk if port_risk > 0 else np.nan

    port_summary.append({
        'plan': plan,
        'return': port_return,
        'risk': port_risk,
        'sharpe': port_sharpe
    })

# 🔹 แปลงเป็น DataFrame และกรองค่าที่ใช้ log ไม่ได้
ef_df = pd.DataFrame(port_summary)
ef_df = ef_df[(ef_df['risk'] > 0) & (ef_df['return'] > 0)]

# 🔹 คัดจุดบนเส้น Efficient Frontier: "return สูงสุดต่อระดับ risk"
ef_sorted = ef_df.sort_values(by='risk')
efficient_points = []
max_return = -np.inf
for _, row in ef_sorted.iterrows():
    if row['return'] > max_return:
        efficient_points.append(row)
        max_return = row['return']
efficient_points_df = pd.DataFrame(efficient_points)

# 🔹 Export จุด Efficient Frontier ไปใช้ต่อใน Power BI
efficient_points_df.to_csv(f'{local_path_opt}efficient_frontier_points_F.csv', index=False, encoding="utf-8-sig")

# 🔹 วาดกราฟ
plt.figure(figsize=(10, 6))

# จุดทั้งหมด
sc = plt.scatter(ef_df['risk'], ef_df['return'], c=ef_df['sharpe'], cmap='viridis', s=5)

# เส้น Efficient Frontier
plt.plot(efficient_points_df['risk'], efficient_points_df['return'], 'r--', linewidth=2, label='Efficient Frontier')

# ------------------plot log scale------------------------
# ปรับแกน log scale
plt.xscale('log')
plt.yscale('log')

# แสดงตัวเลขเป็นทศนิยม (ไม่ใช้ exponential)
formatter = ScalarFormatter()
formatter.set_scientific(False)
formatter.set_useOffset(False)
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

# อื่น ๆ
plt.colorbar(sc, label='Sharpe Ratio')
plt.xlabel('Portfolio Risk (Std) [Log Scale]')
plt.ylabel('Portfolio Expected Return [Log Scale]')
plt.title(f'Efficient Frontier of Optimized Plans F (Log-Log Scale)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.tight_layout()
# plt.show()
# --------------------------------------------------------

## correlation coefficient
import pandas as pd
import numpy as np
import ast
import seaborn as sns
import matplotlib
matplotlib.use('Agg') # บังคับโหมดรันเบื้องหลัง (ไม่มีหน้าจอ)
import matplotlib.pyplot as plt

# 1. โหลดข้อมูล
df = df_filtered.copy()

# 2. ฟังก์ชันสำหรับดึงข้อมูล Yearly Returns ออกมาเป็นชุดตัวเลข
def extract_yearly_returns(json_str):
    try:
        data_list = ast.literal_eval(json_str)
        # ดึงเฉพาะค่า value (return) ออกมาเป็น list
        return [item['value'] for item in data_list]
    except:
        return None

# 3. เตรียมข้อมูล (ตัวอย่าง: คัดมาเฉพาะกองที่มี Sharpe Ratio สูงสุด 500 อันดับแรกเพื่อประมวลผล)
top_funds = df.nlargest(500, 'sharpe_ratio_1y')[['short_code', 'returns_year']]

# 4. สร้างตารางใหม่ที่ Column คือชื่อกองทุน และ Row คือผลตอบแทนแต่ละปี
returns_dict = {}
for idx, row in top_funds.iterrows():
    y_returns = extract_yearly_returns(row['returns_year'])
    if y_returns:
        returns_dict[row['short_code']] = y_returns

df_corr_input = pd.DataFrame(returns_dict)

# 5. คำนวณ Correlation
correlation_matrix = df_corr_input.corr()

# 6. บันทึกเป็น CSV
correlation_matrix.to_csv(f'{local_path_opt}correlation_coefficient_F.csv', encoding='utf-8-sig')

# 7. พล็อต Heatmap
plt.figure(figsize=(24, 20))

sns.heatmap(
    correlation_matrix, 
    annot=True,            # แสดงตัวเลข
    cmap='RdYlGn',         # สีแดง-เหลือง-เขียว
    fmt=".2f",             # ทศนิยม 2 ตำแหน่ง
    annot_kws={"size": 6}, # ** ปรับขนาดตัวเลขในช่องให้เล็กลงพอดีกับช่อง เพื่อให้อ่านง่าย **
    linewidths=0.0,        # เพิ่มเส้นคั่นบางๆ ให้แยกช่องชัดเจน
    cbar_kws={"label": "Correlation Coefficient"} # ใส่ชื่อบอกว่าแถบขวาคืออะไร
)

plt.title(f'Correlation Matrix of Funds F (Based on 10-Year Annual Returns)', fontsize=20, pad=20)
plt.xticks(rotation=90, fontsize=9) # หมุนชื่อกองทุนแนวตั้ง
plt.yticks(rotation=0, fontsize=9)  # ชื่อกองทุนแนวนอน
plt.tight_layout()

# แสดงผล
# plt.show()

# --------------------------------------------------------
# Optimize G
## filter file for optimized
df_filtered = []

# อ่านไฟล์ merged
file_path = f'{local_path}merged_fund_for_optimzed.csv'
df_perf = pd.read_csv(file_path)

# ✅ กรองข้อมูลตามเงื่อนไข
df_filtered = df_perf[
    (df_perf['amc_name_en'] == 'MFC') &
    # (df_perf['dividend_policy'] == 'ไม่จ่าย') &
    (df_perf['fund_tax_type'].isna()) &
    (~df_perf['short_code'].str.contains('PVD', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('สถาบัน', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('ห้ามขายผู้ลงทุนรายย่อย', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('เพื่อผู้ลงทุนนิติบุคคล', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('อัตโน', case=False, na=False))
]

# แสดงผลลัพธ์บางส่วน
print(df_filtered.head())

# แสดงจำนวนกองทุนทั้งหมดที่ตรงเงื่อนไข
print(f"✅ จำนวนกองทุนที่ตรงเงื่อนไขทั้งหมด: {len(df_filtered)}")

## Optimize to file
from scipy.optimize import minimize
from sklearn.utils import resample

# 🔹 เปลี่ยนจากอ่านไฟล์เป็นใช้ df_filtered แทน
df = df_filtered.copy()

# 📌 ตั้งชื่อ column และเลือก field ช่วงเวลามาคำนวน
col_fund = "short_code" # เปลี่ยนชื่อแล้วที่ด้านบน "data.short_code"
col_return = "total_return_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.total_return_1y"
col_std = "std_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.std_1y"

# 📌 ล้างค่าความเสี่ยงต่ำผิดปกติ
df = df[[col_fund, col_return, col_std]].dropna()
df[col_std] = df[col_std].clip(lower=1e-6)

portfolios = []

# ------------------------
# 📌 ฟังก์ชัน optimization (maximize Sharpe = minimize -Sharpe)
def neg_sharpe(weights, returns, risks):
    port_return = np.dot(weights, returns)
    port_std = np.sqrt(np.dot(weights**2, risks**2))  # simplified std
    return -port_return / port_std

# ------------------------
# 📌 สร้าง 10000 แผนพอร์ต (10 กองทุนต่อแผน)
for i in range(10000): # <<<<< ปรับจำนวนแผน
    sample = resample(df, n_samples=10, replace=False, random_state=i) # <<<<< ปรับจำนวนกอง
    codes = sample[col_fund].values
    returns = sample[col_return].values
    risks = sample[col_std].values

    w0 = np.ones(10) / 10 # <<<<< ปรับจำนวนกอง
    bounds = [(0, 0.3)] * 10 # <<<<< ปรับจำนวนกอง, กำหนดขั้นต่ำและเพดานสูงสุดต่อกอง
    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

    result = minimize(neg_sharpe, w0, args=(returns, risks), method='SLSQP',
                      bounds=bounds, constraints=constraints)

    if result.success:
        weights = result.x
        df_out = pd.DataFrame({
            'fund_code': codes,
            'weight': weights,
            'expected_return': returns,
            'risk_std': risks
        })
        df_out['plan'] = f'Plan {i+1}'
        df_out['sharpe_ratio'] = df_out['expected_return'] / df_out['risk_std']
        portfolios.append(df_out)
    else:
        print(f"❌ Optimization failed for Plan {i+1}")

# ------------------------
# 📌 รวมและบันทึก
if portfolios:
    final_df = pd.concat(portfolios)
    final_df.to_csv(f'{local_path_opt}optimized_portfolios_plan_G.csv', index=False, encoding="utf-8-sig")
    print(f"✅ บันทึกไฟล์: optimized_portfolios_plan_G.csv")
else:
    print("❌ ไม่มีพอร์ตที่ optimize สำเร็จ")

## top frequency ranking
# 1. คำนวณหาค่าเฉลี่ยของแต่ละแผน (Plan Summary)
plan_summary = final_df.groupby('plan').agg({
    'sharpe_ratio': 'mean', # หรือจะใช้พอร์ต Sharpe ที่คำนวณจากก้อนรวมก็ได้
    'expected_return': 'sum', # ผลตอบแทนคาดการณ์ของพอร์ต (ถ้าถ่วงน้ำหนักแล้ว)
}).reset_index()

# 2. คัดเลือกเฉพาะ Top 1% ของแผนที่ดีที่สุด (เช่น 100 แผนจาก 10,000)
top_plans_threshold = plan_summary['sharpe_ratio'].quantile(0.99)
top_plans = plan_summary[plan_summary['sharpe_ratio'] >= top_plans_threshold]['plan']

# 3. ดูว่าในแผนระดับ Top เหล่านี้ มีกองทุนไหนปรากฏตัวบ่อยที่สุด
top_funds_analysis = final_df[final_df['plan'].isin(top_plans)]

# นับจำนวนครั้งที่ถูกรับเลือก และน้ำหนักเฉลี่ยที่ถูกใส่ในพอร์ต
fund_stats = top_funds_analysis.groupby('fund_code').agg({
    'weight': ['count', 'mean', 'sum'],
    'expected_return': 'first',
    'risk_std': 'first'
}).reset_index()

# จัดรูปแบบตารางให้ดูง่าย
fund_stats.columns = ['fund_code', 'appearances', 'avg_weight', 'total_weight_impact', 'return', 'risk']
fund_stats = fund_stats.sort_values(by='appearances', ascending=False)

print("⭐ กองทุนที่ปรากฏในแผนระดับ Top 1% บ่อยที่สุด:")
print(fund_stats.head(10))

# บันทึกผลออกมาดู
fund_stats.to_csv(f'{local_path_opt}top_frequency_ranking_G.csv', index=False, encoding="utf-8-sig")

## graph ef + export ef
#Plot กราฟข้อมูล optimized_portfolios + csv
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# 🔹 โหลดไฟล์ข้อมูลแผนพอร์ตที่ optimize แล้ว
file_path = f'{local_path_opt}optimized_portfolios_plan_G.csv'
df = pd.read_csv(file_path)

# 🔹 คำนวณ Risk / Return / Sharpe Ratio ของแต่ละพอร์ต
port_summary = []
for plan, group in df.groupby("plan"):
    weights = group['weight'].values
    returns = group['expected_return'].values
    risks = group['risk_std'].values

    port_return = np.dot(weights, returns)
    port_risk = np.sqrt(np.dot(weights**2, risks**2))
    port_sharpe = port_return / port_risk if port_risk > 0 else np.nan

    port_summary.append({
        'plan': plan,
        'return': port_return,
        'risk': port_risk,
        'sharpe': port_sharpe
    })

# 🔹 แปลงเป็น DataFrame และกรองค่าที่ใช้ log ไม่ได้
ef_df = pd.DataFrame(port_summary)
ef_df = ef_df[(ef_df['risk'] > 0) & (ef_df['return'] > 0)]

# 🔹 คัดจุดบนเส้น Efficient Frontier: "return สูงสุดต่อระดับ risk"
ef_sorted = ef_df.sort_values(by='risk')
efficient_points = []
max_return = -np.inf
for _, row in ef_sorted.iterrows():
    if row['return'] > max_return:
        efficient_points.append(row)
        max_return = row['return']
efficient_points_df = pd.DataFrame(efficient_points)

# 🔹 Export จุด Efficient Frontier ไปใช้ต่อใน Power BI
efficient_points_df.to_csv(f'{local_path_opt}efficient_frontier_points_G.csv', index=False, encoding="utf-8-sig")

# 🔹 วาดกราฟ
plt.figure(figsize=(10, 6))

# จุดทั้งหมด
sc = plt.scatter(ef_df['risk'], ef_df['return'], c=ef_df['sharpe'], cmap='viridis', s=5)

# เส้น Efficient Frontier
plt.plot(efficient_points_df['risk'], efficient_points_df['return'], 'r--', linewidth=2, label='Efficient Frontier')

# ------------------plot log scale------------------------
# ปรับแกน log scale
plt.xscale('log')
plt.yscale('log')

# แสดงตัวเลขเป็นทศนิยม (ไม่ใช้ exponential)
formatter = ScalarFormatter()
formatter.set_scientific(False)
formatter.set_useOffset(False)
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

# อื่น ๆ
plt.colorbar(sc, label='Sharpe Ratio')
plt.xlabel('Portfolio Risk (Std) [Log Scale]')
plt.ylabel('Portfolio Expected Return [Log Scale]')
plt.title(f'Efficient Frontier of Optimized Plans G (Log-Log Scale)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.tight_layout()
# plt.show()
# --------------------------------------------------------

## correlation coefficient
import pandas as pd
import numpy as np
import ast
import seaborn as sns
import matplotlib
matplotlib.use('Agg') # บังคับโหมดรันเบื้องหลัง (ไม่มีหน้าจอ)
import matplotlib.pyplot as plt

# 1. โหลดข้อมูล
df = df_filtered.copy()

# 2. ฟังก์ชันสำหรับดึงข้อมูล Yearly Returns ออกมาเป็นชุดตัวเลข
def extract_yearly_returns(json_str):
    try:
        data_list = ast.literal_eval(json_str)
        # ดึงเฉพาะค่า value (return) ออกมาเป็น list
        return [item['value'] for item in data_list]
    except:
        return None

# 3. เตรียมข้อมูล (ตัวอย่าง: คัดมาเฉพาะกองที่มี Sharpe Ratio สูงสุด 500 อันดับแรกเพื่อประมวลผล)
top_funds = df.nlargest(500, 'sharpe_ratio_1y')[['short_code', 'returns_year']]

# 4. สร้างตารางใหม่ที่ Column คือชื่อกองทุน และ Row คือผลตอบแทนแต่ละปี
returns_dict = {}
for idx, row in top_funds.iterrows():
    y_returns = extract_yearly_returns(row['returns_year'])
    if y_returns:
        returns_dict[row['short_code']] = y_returns

df_corr_input = pd.DataFrame(returns_dict)

# 5. คำนวณ Correlation
correlation_matrix = df_corr_input.corr()

# 6. บันทึกเป็น CSV
correlation_matrix.to_csv(f'{local_path_opt}correlation_coefficient_G.csv', encoding='utf-8-sig')

# 7. พล็อต Heatmap
plt.figure(figsize=(24, 20))

sns.heatmap(
    correlation_matrix, 
    annot=True,            # แสดงตัวเลข
    cmap='RdYlGn',         # สีแดง-เหลือง-เขียว
    fmt=".2f",             # ทศนิยม 2 ตำแหน่ง
    annot_kws={"size": 6}, # ** ปรับขนาดตัวเลขในช่องให้เล็กลงพอดีกับช่อง เพื่อให้อ่านง่าย **
    linewidths=0.0,        # เพิ่มเส้นคั่นบางๆ ให้แยกช่องชัดเจน
    cbar_kws={"label": "Correlation Coefficient"} # ใส่ชื่อบอกว่าแถบขวาคืออะไร
)

plt.title(f'Correlation Matrix of Funds G (Based on 10-Year Annual Returns)', fontsize=20, pad=20)
plt.xticks(rotation=90, fontsize=9) # หมุนชื่อกองทุนแนวตั้ง
plt.yticks(rotation=0, fontsize=9)  # ชื่อกองทุนแนวนอน
plt.tight_layout()

# แสดงผล
# plt.show()

# --------------------------------------------------------
# Optimize H
## filter file for optimized
df_filtered = []

# อ่านไฟล์ merged
file_path = f'{local_path}merged_fund_for_optimzed.csv'
df_perf = pd.read_csv(file_path)

# ✅ กรองข้อมูลตามเงื่อนไข
df_filtered = df_perf[
    (df_perf['amc_name_en'] == 'LHFUND') &
    # (df_perf['dividend_policy'] == 'ไม่จ่าย') &
    (df_perf['fund_tax_type'].isna()) &
    (~df_perf['short_code'].str.contains('PVD', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('สถาบัน', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('ห้ามขายผู้ลงทุนรายย่อย', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('เพื่อผู้ลงทุนนิติบุคคล', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('อัตโน', case=False, na=False))
]

# แสดงผลลัพธ์บางส่วน
print(df_filtered.head())

# แสดงจำนวนกองทุนทั้งหมดที่ตรงเงื่อนไข
print(f"✅ จำนวนกองทุนที่ตรงเงื่อนไขทั้งหมด: {len(df_filtered)}")

## Optimize to file
from scipy.optimize import minimize
from sklearn.utils import resample

# 🔹 เปลี่ยนจากอ่านไฟล์เป็นใช้ df_filtered แทน
df = df_filtered.copy()

# 📌 ตั้งชื่อ column และเลือก field ช่วงเวลามาคำนวน
col_fund = "short_code" # เปลี่ยนชื่อแล้วที่ด้านบน "data.short_code"
col_return = "total_return_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.total_return_1y"
col_std = "std_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.std_1y"

# 📌 ล้างค่าความเสี่ยงต่ำผิดปกติ
df = df[[col_fund, col_return, col_std]].dropna()
df[col_std] = df[col_std].clip(lower=1e-6)

portfolios = []

# ------------------------
# 📌 ฟังก์ชัน optimization (maximize Sharpe = minimize -Sharpe)
def neg_sharpe(weights, returns, risks):
    port_return = np.dot(weights, returns)
    port_std = np.sqrt(np.dot(weights**2, risks**2))  # simplified std
    return -port_return / port_std

# ------------------------
# 📌 สร้าง 10000 แผนพอร์ต (10 กองทุนต่อแผน)
for i in range(10000): # <<<<< ปรับจำนวนแผน
    sample = resample(df, n_samples=10, replace=False, random_state=i) # <<<<< ปรับจำนวนกอง
    codes = sample[col_fund].values
    returns = sample[col_return].values
    risks = sample[col_std].values

    w0 = np.ones(10) / 10 # <<<<< ปรับจำนวนกอง
    bounds = [(0, 0.3)] * 10 # <<<<< ปรับจำนวนกอง, กำหนดขั้นต่ำและเพดานสูงสุดต่อกอง
    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

    result = minimize(neg_sharpe, w0, args=(returns, risks), method='SLSQP',
                      bounds=bounds, constraints=constraints)

    if result.success:
        weights = result.x
        df_out = pd.DataFrame({
            'fund_code': codes,
            'weight': weights,
            'expected_return': returns,
            'risk_std': risks
        })
        df_out['plan'] = f'Plan {i+1}'
        df_out['sharpe_ratio'] = df_out['expected_return'] / df_out['risk_std']
        portfolios.append(df_out)
    else:
        print(f"❌ Optimization failed for Plan {i+1}")

# ------------------------
# 📌 รวมและบันทึก
if portfolios:
    final_df = pd.concat(portfolios)
    final_df.to_csv(f'{local_path_opt}optimized_portfolios_plan_H.csv', index=False, encoding="utf-8-sig")
    print(f"✅ บันทึกไฟล์: optimized_portfolios_plan_H.csv")
else:
    print("❌ ไม่มีพอร์ตที่ optimize สำเร็จ")

## top frequency ranking
# 1. คำนวณหาค่าเฉลี่ยของแต่ละแผน (Plan Summary)
plan_summary = final_df.groupby('plan').agg({
    'sharpe_ratio': 'mean', # หรือจะใช้พอร์ต Sharpe ที่คำนวณจากก้อนรวมก็ได้
    'expected_return': 'sum', # ผลตอบแทนคาดการณ์ของพอร์ต (ถ้าถ่วงน้ำหนักแล้ว)
}).reset_index()

# 2. คัดเลือกเฉพาะ Top 1% ของแผนที่ดีที่สุด (เช่น 100 แผนจาก 10,000)
top_plans_threshold = plan_summary['sharpe_ratio'].quantile(0.99)
top_plans = plan_summary[plan_summary['sharpe_ratio'] >= top_plans_threshold]['plan']

# 3. ดูว่าในแผนระดับ Top เหล่านี้ มีกองทุนไหนปรากฏตัวบ่อยที่สุด
top_funds_analysis = final_df[final_df['plan'].isin(top_plans)]

# นับจำนวนครั้งที่ถูกรับเลือก และน้ำหนักเฉลี่ยที่ถูกใส่ในพอร์ต
fund_stats = top_funds_analysis.groupby('fund_code').agg({
    'weight': ['count', 'mean', 'sum'],
    'expected_return': 'first',
    'risk_std': 'first'
}).reset_index()

# จัดรูปแบบตารางให้ดูง่าย
fund_stats.columns = ['fund_code', 'appearances', 'avg_weight', 'total_weight_impact', 'return', 'risk']
fund_stats = fund_stats.sort_values(by='appearances', ascending=False)

print("⭐ กองทุนที่ปรากฏในแผนระดับ Top 1% บ่อยที่สุด:")
print(fund_stats.head(10))

# บันทึกผลออกมาดู
fund_stats.to_csv(f'{local_path_opt}top_frequency_ranking_H.csv', index=False, encoding="utf-8-sig")

## graph ef + export ef
#Plot กราฟข้อมูล optimized_portfolios + csv
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# 🔹 โหลดไฟล์ข้อมูลแผนพอร์ตที่ optimize แล้ว
file_path = f'{local_path_opt}optimized_portfolios_plan_H.csv'
df = pd.read_csv(file_path)

# 🔹 คำนวณ Risk / Return / Sharpe Ratio ของแต่ละพอร์ต
port_summary = []
for plan, group in df.groupby("plan"):
    weights = group['weight'].values
    returns = group['expected_return'].values
    risks = group['risk_std'].values

    port_return = np.dot(weights, returns)
    port_risk = np.sqrt(np.dot(weights**2, risks**2))
    port_sharpe = port_return / port_risk if port_risk > 0 else np.nan

    port_summary.append({
        'plan': plan,
        'return': port_return,
        'risk': port_risk,
        'sharpe': port_sharpe
    })

# 🔹 แปลงเป็น DataFrame และกรองค่าที่ใช้ log ไม่ได้
ef_df = pd.DataFrame(port_summary)
ef_df = ef_df[(ef_df['risk'] > 0) & (ef_df['return'] > 0)]

# 🔹 คัดจุดบนเส้น Efficient Frontier: "return สูงสุดต่อระดับ risk"
ef_sorted = ef_df.sort_values(by='risk')
efficient_points = []
max_return = -np.inf
for _, row in ef_sorted.iterrows():
    if row['return'] > max_return:
        efficient_points.append(row)
        max_return = row['return']
efficient_points_df = pd.DataFrame(efficient_points)

# 🔹 Export จุด Efficient Frontier ไปใช้ต่อใน Power BI
efficient_points_df.to_csv(f'{local_path_opt}efficient_frontier_points_H.csv', index=False, encoding="utf-8-sig")

# 🔹 วาดกราฟ
plt.figure(figsize=(10, 6))

# จุดทั้งหมด
sc = plt.scatter(ef_df['risk'], ef_df['return'], c=ef_df['sharpe'], cmap='viridis', s=5)

# เส้น Efficient Frontier
plt.plot(efficient_points_df['risk'], efficient_points_df['return'], 'r--', linewidth=2, label='Efficient Frontier')

# ------------------plot log scale------------------------
# ปรับแกน log scale
plt.xscale('log')
plt.yscale('log')

# แสดงตัวเลขเป็นทศนิยม (ไม่ใช้ exponential)
formatter = ScalarFormatter()
formatter.set_scientific(False)
formatter.set_useOffset(False)
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

# อื่น ๆ
plt.colorbar(sc, label='Sharpe Ratio')
plt.xlabel('Portfolio Risk (Std) [Log Scale]')
plt.ylabel('Portfolio Expected Return [Log Scale]')
plt.title(f'Efficient Frontier of Optimized Plans H (Log-Log Scale)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.tight_layout()
# plt.show()
# --------------------------------------------------------

## correlation coefficient
import pandas as pd
import numpy as np
import ast
import seaborn as sns
import matplotlib
matplotlib.use('Agg') # บังคับโหมดรันเบื้องหลัง (ไม่มีหน้าจอ)
import matplotlib.pyplot as plt

# 1. โหลดข้อมูล
df = df_filtered.copy()

# 2. ฟังก์ชันสำหรับดึงข้อมูล Yearly Returns ออกมาเป็นชุดตัวเลข
def extract_yearly_returns(json_str):
    try:
        data_list = ast.literal_eval(json_str)
        # ดึงเฉพาะค่า value (return) ออกมาเป็น list
        return [item['value'] for item in data_list]
    except:
        return None

# 3. เตรียมข้อมูล (ตัวอย่าง: คัดมาเฉพาะกองที่มี Sharpe Ratio สูงสุด 500 อันดับแรกเพื่อประมวลผล)
top_funds = df.nlargest(500, 'sharpe_ratio_1y')[['short_code', 'returns_year']]

# 4. สร้างตารางใหม่ที่ Column คือชื่อกองทุน และ Row คือผลตอบแทนแต่ละปี
returns_dict = {}
for idx, row in top_funds.iterrows():
    y_returns = extract_yearly_returns(row['returns_year'])
    if y_returns:
        returns_dict[row['short_code']] = y_returns

df_corr_input = pd.DataFrame(returns_dict)

# 5. คำนวณ Correlation
correlation_matrix = df_corr_input.corr()

# 6. บันทึกเป็น CSV
correlation_matrix.to_csv(f'{local_path_opt}correlation_coefficient_H.csv', encoding='utf-8-sig')

# 7. พล็อต Heatmap
plt.figure(figsize=(24, 20))

sns.heatmap(
    correlation_matrix, 
    annot=True,            # แสดงตัวเลข
    cmap='RdYlGn',         # สีแดง-เหลือง-เขียว
    fmt=".2f",             # ทศนิยม 2 ตำแหน่ง
    annot_kws={"size": 6}, # ** ปรับขนาดตัวเลขในช่องให้เล็กลงพอดีกับช่อง เพื่อให้อ่านง่าย **
    linewidths=0.0,        # เพิ่มเส้นคั่นบางๆ ให้แยกช่องชัดเจน
    cbar_kws={"label": "Correlation Coefficient"} # ใส่ชื่อบอกว่าแถบขวาคืออะไร
)

plt.title(f'Correlation Matrix of Funds H (Based on 10-Year Annual Returns)', fontsize=20, pad=20)
plt.xticks(rotation=90, fontsize=9) # หมุนชื่อกองทุนแนวตั้ง
plt.yticks(rotation=0, fontsize=9)  # ชื่อกองทุนแนวนอน
plt.tight_layout()

# แสดงผล
# plt.show()

# --------------------------------------------------------
# Optimize I
## filter file for optimized
df_filtered = []

# อ่านไฟล์ merged
file_path = f'{local_path}merged_fund_for_optimzed.csv'
df_perf = pd.read_csv(file_path)

# ✅ กรองข้อมูลตามเงื่อนไข
df_filtered = df_perf[
    (df_perf['amc_name_en'] == 'UOBAM') &
    # (df_perf['dividend_policy'] == 'ไม่จ่าย') &
    (df_perf['fund_tax_type'].isna()) &
    (~df_perf['short_code'].str.contains('PVD', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('สถาบัน', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('ห้ามขายผู้ลงทุนรายย่อย', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('เพื่อผู้ลงทุนนิติบุคคล', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('อัตโน', case=False, na=False))
]

# แสดงผลลัพธ์บางส่วน
print(df_filtered.head())

# แสดงจำนวนกองทุนทั้งหมดที่ตรงเงื่อนไข
print(f"✅ จำนวนกองทุนที่ตรงเงื่อนไขทั้งหมด: {len(df_filtered)}")

## Optimize to file
from scipy.optimize import minimize
from sklearn.utils import resample

# 🔹 เปลี่ยนจากอ่านไฟล์เป็นใช้ df_filtered แทน
df = df_filtered.copy()

# 📌 ตั้งชื่อ column และเลือก field ช่วงเวลามาคำนวน
col_fund = "short_code" # เปลี่ยนชื่อแล้วที่ด้านบน "data.short_code"
col_return = "total_return_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.total_return_1y"
col_std = "std_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.std_1y"

# 📌 ล้างค่าความเสี่ยงต่ำผิดปกติ
df = df[[col_fund, col_return, col_std]].dropna()
df[col_std] = df[col_std].clip(lower=1e-6)

portfolios = []

# ------------------------
# 📌 ฟังก์ชัน optimization (maximize Sharpe = minimize -Sharpe)
def neg_sharpe(weights, returns, risks):
    port_return = np.dot(weights, returns)
    port_std = np.sqrt(np.dot(weights**2, risks**2))  # simplified std
    return -port_return / port_std

# ------------------------
# 📌 สร้าง 10000 แผนพอร์ต (10 กองทุนต่อแผน)
for i in range(10000): # <<<<< ปรับจำนวนแผน
    sample = resample(df, n_samples=10, replace=False, random_state=i) # <<<<< ปรับจำนวนกอง
    codes = sample[col_fund].values
    returns = sample[col_return].values
    risks = sample[col_std].values

    w0 = np.ones(10) / 10 # <<<<< ปรับจำนวนกอง
    bounds = [(0, 0.3)] * 10 # <<<<< ปรับจำนวนกอง, กำหนดขั้นต่ำและเพดานสูงสุดต่อกอง
    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

    result = minimize(neg_sharpe, w0, args=(returns, risks), method='SLSQP',
                      bounds=bounds, constraints=constraints)

    if result.success:
        weights = result.x
        df_out = pd.DataFrame({
            'fund_code': codes,
            'weight': weights,
            'expected_return': returns,
            'risk_std': risks
        })
        df_out['plan'] = f'Plan {i+1}'
        df_out['sharpe_ratio'] = df_out['expected_return'] / df_out['risk_std']
        portfolios.append(df_out)
    else:
        print(f"❌ Optimization failed for Plan {i+1}")

# ------------------------
# 📌 รวมและบันทึก
if portfolios:
    final_df = pd.concat(portfolios)
    final_df.to_csv(f'{local_path_opt}optimized_portfolios_plan_I.csv', index=False, encoding="utf-8-sig")
    print(f"✅ บันทึกไฟล์: optimized_portfolios_plan_I.csv")
else:
    print("❌ ไม่มีพอร์ตที่ optimize สำเร็จ")

## top frequency ranking
# 1. คำนวณหาค่าเฉลี่ยของแต่ละแผน (Plan Summary)
plan_summary = final_df.groupby('plan').agg({
    'sharpe_ratio': 'mean', # หรือจะใช้พอร์ต Sharpe ที่คำนวณจากก้อนรวมก็ได้
    'expected_return': 'sum', # ผลตอบแทนคาดการณ์ของพอร์ต (ถ้าถ่วงน้ำหนักแล้ว)
}).reset_index()

# 2. คัดเลือกเฉพาะ Top 1% ของแผนที่ดีที่สุด (เช่น 100 แผนจาก 10,000)
top_plans_threshold = plan_summary['sharpe_ratio'].quantile(0.99)
top_plans = plan_summary[plan_summary['sharpe_ratio'] >= top_plans_threshold]['plan']

# 3. ดูว่าในแผนระดับ Top เหล่านี้ มีกองทุนไหนปรากฏตัวบ่อยที่สุด
top_funds_analysis = final_df[final_df['plan'].isin(top_plans)]

# นับจำนวนครั้งที่ถูกรับเลือก และน้ำหนักเฉลี่ยที่ถูกใส่ในพอร์ต
fund_stats = top_funds_analysis.groupby('fund_code').agg({
    'weight': ['count', 'mean', 'sum'],
    'expected_return': 'first',
    'risk_std': 'first'
}).reset_index()

# จัดรูปแบบตารางให้ดูง่าย
fund_stats.columns = ['fund_code', 'appearances', 'avg_weight', 'total_weight_impact', 'return', 'risk']
fund_stats = fund_stats.sort_values(by='appearances', ascending=False)

print("⭐ กองทุนที่ปรากฏในแผนระดับ Top 1% บ่อยที่สุด:")
print(fund_stats.head(10))

# บันทึกผลออกมาดู
fund_stats.to_csv(f'{local_path_opt}top_frequency_ranking_I.csv', index=False, encoding="utf-8-sig")

## graph ef + export ef
#Plot กราฟข้อมูล optimized_portfolios + csv
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# 🔹 โหลดไฟล์ข้อมูลแผนพอร์ตที่ optimize แล้ว
file_path = f'{local_path_opt}optimized_portfolios_plan_I.csv'
df = pd.read_csv(file_path)

# 🔹 คำนวณ Risk / Return / Sharpe Ratio ของแต่ละพอร์ต
port_summary = []
for plan, group in df.groupby("plan"):
    weights = group['weight'].values
    returns = group['expected_return'].values
    risks = group['risk_std'].values

    port_return = np.dot(weights, returns)
    port_risk = np.sqrt(np.dot(weights**2, risks**2))
    port_sharpe = port_return / port_risk if port_risk > 0 else np.nan

    port_summary.append({
        'plan': plan,
        'return': port_return,
        'risk': port_risk,
        'sharpe': port_sharpe
    })

# 🔹 แปลงเป็น DataFrame และกรองค่าที่ใช้ log ไม่ได้
ef_df = pd.DataFrame(port_summary)
ef_df = ef_df[(ef_df['risk'] > 0) & (ef_df['return'] > 0)]

# 🔹 คัดจุดบนเส้น Efficient Frontier: "return สูงสุดต่อระดับ risk"
ef_sorted = ef_df.sort_values(by='risk')
efficient_points = []
max_return = -np.inf
for _, row in ef_sorted.iterrows():
    if row['return'] > max_return:
        efficient_points.append(row)
        max_return = row['return']
efficient_points_df = pd.DataFrame(efficient_points)

# 🔹 Export จุด Efficient Frontier ไปใช้ต่อใน Power BI
efficient_points_df.to_csv(f'{local_path_opt}efficient_frontier_points_I.csv', index=False, encoding="utf-8-sig")

# 🔹 วาดกราฟ
plt.figure(figsize=(10, 6))

# จุดทั้งหมด
sc = plt.scatter(ef_df['risk'], ef_df['return'], c=ef_df['sharpe'], cmap='viridis', s=5)

# เส้น Efficient Frontier
plt.plot(efficient_points_df['risk'], efficient_points_df['return'], 'r--', linewidth=2, label='Efficient Frontier')

# ------------------plot log scale------------------------
# ปรับแกน log scale
plt.xscale('log')
plt.yscale('log')

# แสดงตัวเลขเป็นทศนิยม (ไม่ใช้ exponential)
formatter = ScalarFormatter()
formatter.set_scientific(False)
formatter.set_useOffset(False)
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

# อื่น ๆ
plt.colorbar(sc, label='Sharpe Ratio')
plt.xlabel('Portfolio Risk (Std) [Log Scale]')
plt.ylabel('Portfolio Expected Return [Log Scale]')
plt.title(f'Efficient Frontier of Optimized Plans I (Log-Log Scale)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.tight_layout()
# plt.show()
# --------------------------------------------------------

## correlation coefficient
import pandas as pd
import numpy as np
import ast
import seaborn as sns
import matplotlib
matplotlib.use('Agg') # บังคับโหมดรันเบื้องหลัง (ไม่มีหน้าจอ)
import matplotlib.pyplot as plt

# 1. โหลดข้อมูล
df = df_filtered.copy()

# 2. ฟังก์ชันสำหรับดึงข้อมูล Yearly Returns ออกมาเป็นชุดตัวเลข
def extract_yearly_returns(json_str):
    try:
        data_list = ast.literal_eval(json_str)
        # ดึงเฉพาะค่า value (return) ออกมาเป็น list
        return [item['value'] for item in data_list]
    except:
        return None

# 3. เตรียมข้อมูล (ตัวอย่าง: คัดมาเฉพาะกองที่มี Sharpe Ratio สูงสุด 500 อันดับแรกเพื่อประมวลผล)
top_funds = df.nlargest(500, 'sharpe_ratio_1y')[['short_code', 'returns_year']]

# 4. สร้างตารางใหม่ที่ Column คือชื่อกองทุน และ Row คือผลตอบแทนแต่ละปี
returns_dict = {}
for idx, row in top_funds.iterrows():
    y_returns = extract_yearly_returns(row['returns_year'])
    if y_returns:
        returns_dict[row['short_code']] = y_returns

df_corr_input = pd.DataFrame(returns_dict)

# 5. คำนวณ Correlation
correlation_matrix = df_corr_input.corr()

# 6. บันทึกเป็น CSV
correlation_matrix.to_csv(f'{local_path_opt}correlation_coefficient_I.csv', encoding='utf-8-sig')

# 7. พล็อต Heatmap
plt.figure(figsize=(24, 20))

sns.heatmap(
    correlation_matrix, 
    annot=True,            # แสดงตัวเลข
    cmap='RdYlGn',         # สีแดง-เหลือง-เขียว
    fmt=".2f",             # ทศนิยม 2 ตำแหน่ง
    annot_kws={"size": 6}, # ** ปรับขนาดตัวเลขในช่องให้เล็กลงพอดีกับช่อง เพื่อให้อ่านง่าย **
    linewidths=0.0,        # เพิ่มเส้นคั่นบางๆ ให้แยกช่องชัดเจน
    cbar_kws={"label": "Correlation Coefficient"} # ใส่ชื่อบอกว่าแถบขวาคืออะไร
)

plt.title(f'Correlation Matrix of Funds I (Based on 10-Year Annual Returns)', fontsize=20, pad=20)
plt.xticks(rotation=90, fontsize=9) # หมุนชื่อกองทุนแนวตั้ง
plt.yticks(rotation=0, fontsize=9)  # ชื่อกองทุนแนวนอน
plt.tight_layout()

# แสดงผล
# plt.show()

# --------------------------------------------------------
# Optimize J
## filter file for optimized
df_filtered = []

# อ่านไฟล์ merged
file_path = f'{local_path}merged_fund_for_optimzed.csv'
df_perf = pd.read_csv(file_path)

# ✅ กรองข้อมูลตามเงื่อนไข
df_filtered = df_perf[
    (df_perf['amc_name_en'] == 'KKPAM') &
    # (df_perf['dividend_policy'] == 'ไม่จ่าย') &
    (df_perf['fund_tax_type'].isna()) &
    (~df_perf['short_code'].str.contains('PVD', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('สถาบัน', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('ห้ามขายผู้ลงทุนรายย่อย', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('เพื่อผู้ลงทุนนิติบุคคล', case=False, na=False)) &
    (~df_perf['name_th'].str.contains('อัตโน', case=False, na=False))
]

# แสดงผลลัพธ์บางส่วน
print(df_filtered.head())

# แสดงจำนวนกองทุนทั้งหมดที่ตรงเงื่อนไข
print(f"✅ จำนวนกองทุนที่ตรงเงื่อนไขทั้งหมด: {len(df_filtered)}")

## Optimize to file
from scipy.optimize import minimize
from sklearn.utils import resample

# 🔹 เปลี่ยนจากอ่านไฟล์เป็นใช้ df_filtered แทน
df = df_filtered.copy()

# 📌 ตั้งชื่อ column และเลือก field ช่วงเวลามาคำนวน
col_fund = "short_code" # เปลี่ยนชื่อแล้วที่ด้านบน "data.short_code"
col_return = "total_return_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.total_return_1y"
col_std = "std_1y" # เปลี่ยนชื่อแล้วที่ด้านบน "data.std_1y"

# 📌 ล้างค่าความเสี่ยงต่ำผิดปกติ
df = df[[col_fund, col_return, col_std]].dropna()
df[col_std] = df[col_std].clip(lower=1e-6)

portfolios = []

# ------------------------
# 📌 ฟังก์ชัน optimization (maximize Sharpe = minimize -Sharpe)
def neg_sharpe(weights, returns, risks):
    port_return = np.dot(weights, returns)
    port_std = np.sqrt(np.dot(weights**2, risks**2))  # simplified std
    return -port_return / port_std

# ------------------------
# 📌 สร้าง 10000 แผนพอร์ต (10 กองทุนต่อแผน)
for i in range(10000): # <<<<< ปรับจำนวนแผน
    sample = resample(df, n_samples=10, replace=False, random_state=i) # <<<<< ปรับจำนวนกอง
    codes = sample[col_fund].values
    returns = sample[col_return].values
    risks = sample[col_std].values

    w0 = np.ones(10) / 10 # <<<<< ปรับจำนวนกอง
    bounds = [(0, 0.3)] * 10 # <<<<< ปรับจำนวนกอง, กำหนดขั้นต่ำและเพดานสูงสุดต่อกอง
    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

    result = minimize(neg_sharpe, w0, args=(returns, risks), method='SLSQP',
                      bounds=bounds, constraints=constraints)

    if result.success:
        weights = result.x
        df_out = pd.DataFrame({
            'fund_code': codes,
            'weight': weights,
            'expected_return': returns,
            'risk_std': risks
        })
        df_out['plan'] = f'Plan {i+1}'
        df_out['sharpe_ratio'] = df_out['expected_return'] / df_out['risk_std']
        portfolios.append(df_out)
    else:
        print(f"❌ Optimization failed for Plan {i+1}")

# ------------------------
# 📌 รวมและบันทึก
if portfolios:
    final_df = pd.concat(portfolios)
    final_df.to_csv(f'{local_path_opt}optimized_portfolios_plan_J.csv', index=False, encoding="utf-8-sig")
    print(f"✅ บันทึกไฟล์: optimized_portfolios_plan_J.csv")
else:
    print("❌ ไม่มีพอร์ตที่ optimize สำเร็จ")

## top frequency ranking
# 1. คำนวณหาค่าเฉลี่ยของแต่ละแผน (Plan Summary)
plan_summary = final_df.groupby('plan').agg({
    'sharpe_ratio': 'mean', # หรือจะใช้พอร์ต Sharpe ที่คำนวณจากก้อนรวมก็ได้
    'expected_return': 'sum', # ผลตอบแทนคาดการณ์ของพอร์ต (ถ้าถ่วงน้ำหนักแล้ว)
}).reset_index()

# 2. คัดเลือกเฉพาะ Top 1% ของแผนที่ดีที่สุด (เช่น 100 แผนจาก 10,000)
top_plans_threshold = plan_summary['sharpe_ratio'].quantile(0.99)
top_plans = plan_summary[plan_summary['sharpe_ratio'] >= top_plans_threshold]['plan']

# 3. ดูว่าในแผนระดับ Top เหล่านี้ มีกองทุนไหนปรากฏตัวบ่อยที่สุด
top_funds_analysis = final_df[final_df['plan'].isin(top_plans)]

# นับจำนวนครั้งที่ถูกรับเลือก และน้ำหนักเฉลี่ยที่ถูกใส่ในพอร์ต
fund_stats = top_funds_analysis.groupby('fund_code').agg({
    'weight': ['count', 'mean', 'sum'],
    'expected_return': 'first',
    'risk_std': 'first'
}).reset_index()

# จัดรูปแบบตารางให้ดูง่าย
fund_stats.columns = ['fund_code', 'appearances', 'avg_weight', 'total_weight_impact', 'return', 'risk']
fund_stats = fund_stats.sort_values(by='appearances', ascending=False)

print("⭐ กองทุนที่ปรากฏในแผนระดับ Top 1% บ่อยที่สุด:")
print(fund_stats.head(10))

# บันทึกผลออกมาดู
fund_stats.to_csv(f'{local_path_opt}top_frequency_ranking_J.csv', index=False, encoding="utf-8-sig")

## graph ef + export ef
#Plot กราฟข้อมูล optimized_portfolios + csv
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# 🔹 โหลดไฟล์ข้อมูลแผนพอร์ตที่ optimize แล้ว
file_path = f'{local_path_opt}optimized_portfolios_plan_J.csv'
df = pd.read_csv(file_path)

# 🔹 คำนวณ Risk / Return / Sharpe Ratio ของแต่ละพอร์ต
port_summary = []
for plan, group in df.groupby("plan"):
    weights = group['weight'].values
    returns = group['expected_return'].values
    risks = group['risk_std'].values

    port_return = np.dot(weights, returns)
    port_risk = np.sqrt(np.dot(weights**2, risks**2))
    port_sharpe = port_return / port_risk if port_risk > 0 else np.nan

    port_summary.append({
        'plan': plan,
        'return': port_return,
        'risk': port_risk,
        'sharpe': port_sharpe
    })

# 🔹 แปลงเป็น DataFrame และกรองค่าที่ใช้ log ไม่ได้
ef_df = pd.DataFrame(port_summary)
ef_df = ef_df[(ef_df['risk'] > 0) & (ef_df['return'] > 0)]

# 🔹 คัดจุดบนเส้น Efficient Frontier: "return สูงสุดต่อระดับ risk"
ef_sorted = ef_df.sort_values(by='risk')
efficient_points = []
max_return = -np.inf
for _, row in ef_sorted.iterrows():
    if row['return'] > max_return:
        efficient_points.append(row)
        max_return = row['return']
efficient_points_df = pd.DataFrame(efficient_points)

# 🔹 Export จุด Efficient Frontier ไปใช้ต่อใน Power BI
efficient_points_df.to_csv(f'{local_path_opt}efficient_frontier_points_J.csv', index=False, encoding="utf-8-sig")

# 🔹 วาดกราฟ
plt.figure(figsize=(10, 6))

# จุดทั้งหมด
sc = plt.scatter(ef_df['risk'], ef_df['return'], c=ef_df['sharpe'], cmap='viridis', s=5)

# เส้น Efficient Frontier
plt.plot(efficient_points_df['risk'], efficient_points_df['return'], 'r--', linewidth=2, label='Efficient Frontier')

# ------------------plot log scale------------------------
# ปรับแกน log scale
plt.xscale('log')
plt.yscale('log')

# แสดงตัวเลขเป็นทศนิยม (ไม่ใช้ exponential)
formatter = ScalarFormatter()
formatter.set_scientific(False)
formatter.set_useOffset(False)
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

# อื่น ๆ
plt.colorbar(sc, label='Sharpe Ratio')
plt.xlabel('Portfolio Risk (Std) [Log Scale]')
plt.ylabel('Portfolio Expected Return [Log Scale]')
plt.title(f'Efficient Frontier of Optimized Plans J (Log-Log Scale)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.tight_layout()
# plt.show()
# --------------------------------------------------------

## correlation coefficient
import pandas as pd
import numpy as np
import ast
import seaborn as sns
import matplotlib
matplotlib.use('Agg') # บังคับโหมดรันเบื้องหลัง (ไม่มีหน้าจอ)
import matplotlib.pyplot as plt

# 1. โหลดข้อมูล
df = df_filtered.copy()

# 2. ฟังก์ชันสำหรับดึงข้อมูล Yearly Returns ออกมาเป็นชุดตัวเลข
def extract_yearly_returns(json_str):
    try:
        data_list = ast.literal_eval(json_str)
        # ดึงเฉพาะค่า value (return) ออกมาเป็น list
        return [item['value'] for item in data_list]
    except:
        return None

# 3. เตรียมข้อมูล (ตัวอย่าง: คัดมาเฉพาะกองที่มี Sharpe Ratio สูงสุด 500 อันดับแรกเพื่อประมวลผล)
top_funds = df.nlargest(500, 'sharpe_ratio_1y')[['short_code', 'returns_year']]

# 4. สร้างตารางใหม่ที่ Column คือชื่อกองทุน และ Row คือผลตอบแทนแต่ละปี
returns_dict = {}
for idx, row in top_funds.iterrows():
    y_returns = extract_yearly_returns(row['returns_year'])
    if y_returns:
        returns_dict[row['short_code']] = y_returns

df_corr_input = pd.DataFrame(returns_dict)

# 5. คำนวณ Correlation
correlation_matrix = df_corr_input.corr()

# 6. บันทึกเป็น CSV
correlation_matrix.to_csv(f'{local_path_opt}correlation_coefficient_J.csv', encoding='utf-8-sig')

# 7. พล็อต Heatmap
plt.figure(figsize=(24, 20))

sns.heatmap(
    correlation_matrix, 
    annot=True,            # แสดงตัวเลข
    cmap='RdYlGn',         # สีแดง-เหลือง-เขียว
    fmt=".2f",             # ทศนิยม 2 ตำแหน่ง
    annot_kws={"size": 6}, # ** ปรับขนาดตัวเลขในช่องให้เล็กลงพอดีกับช่อง เพื่อให้อ่านง่าย **
    linewidths=0.0,        # เพิ่มเส้นคั่นบางๆ ให้แยกช่องชัดเจน
    cbar_kws={"label": "Correlation Coefficient"} # ใส่ชื่อบอกว่าแถบขวาคืออะไร
)

plt.title(f'Correlation Matrix of Funds J (Based on 10-Year Annual Returns)', fontsize=20, pad=20)
plt.xticks(rotation=90, fontsize=9) # หมุนชื่อกองทุนแนวตั้ง
plt.yticks(rotation=0, fontsize=9)  # ชื่อกองทุนแนวนอน
plt.tight_layout()

# แสดงผล
# plt.show()

# --------------------------------------------------------
