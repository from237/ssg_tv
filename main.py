import requests
from bs4 import BeautifulSoup
import csv
import time
import random
from datetime import date, timedelta, datetime
import urllib3
from collections import Counter
import os
import pandas as pd
import numpy as np

# 보안 경고 끄기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# [설정] 파일 및 폴더 경로
# ==========================================
FILENAME = "data.csv"
WEIGHT_FOLDER = "weights"
LOADED_WEIGHTS_MAP = {}

# ==========================================
# [설정] 하드코딩된 매핑 데이터
# ==========================================
PB_BRANDS = ["에디티드", "에디션S", "블루핏", "여유", "엘라코닉"]

CATEGORY_MAP = {
    '가공식품건강식품': '건강식품', '패션여성의류': '여성의류', '패션레포츠의류': '레포츠', 
    '신선식품농산물': '일반식품', '뷰티기초화장품': '뷰티', '뷰티색조화장품': '뷰티', 
    '가공식품냉동식품': '일반식품', '가공식품즉석/편의식품': '일반식품', '패션남성캐쥬얼': '캐쥬얼남성', 
    '패션잡화시계/쥬얼리': '잡화', '패션잡화잡화': '잡화', '뷰티헤어/바디용품': '뷰티', 
    '무형서비스보험금융': '보험', '생활용품세탁용품': '생활용품', '패션잡화언더웨어': '언더웨어', 
    '가공식품축산가공식품': '일반식품', '패션레포츠용품': '레포츠', '가전/디지털주방가전': '주방가전', 
    '가공식품조미료': '일반식품', '생활용품위생용품': '생활용품', '무형서비스렌탈및기타 서비스': '렌탈', 
    '생활용품주방용품': '주방용품', '생활용품청소/욕실용품': '생활용품', '패션남성클래식': '캐쥬얼남성', 
    '신선식품수산물': '일반식품', '가구/인테리어침구단품': '침구', '뷰티이미용기기': '뷰티', 
    '패션유니섹스': '캐쥬얼남성', '생활용품생활용품': '생활용품', '가공식품빵류/떡류': '일반식품', 
    '신선식품축산물': '일반식품', '가공식품절임/발효식품': '일반식품', '가공식품어육/연식품류': '일반식품', 
    '패션잡화신발': '잡화', '신선식품신선식품세트류': '일반식품', '스포츠/레저헬스': '레포츠', 
    '스포츠/레저골프': '레포츠', '생활용품의료기기': '생활가전', '무형서비스여행/예약서비스': '여행', 
    '가구/인테리어침실가구': '가구', '가구/인테리어거실가구': '가구', '가구/인테리어인테리어소품': '침구', 
    '가구/인테리어침구세트': '침구', '가공식품음료류': '일반식품', '교육/문화문구/사무용품': '생활용품', 
    '생활용품의료용품': '생활가전', '가공식품과자류': '일반식품', '가공식품수산가공식품': '일반식품', 
    '가전/디지털생활가전': '생활가전', '스포츠/레저등산': '레포츠'
}

# ==========================================
# [함수] 로직 모음
# ==========================================
def determine_md_class(brand, cat1, cat2):
    clean_brand = str(brand).replace(" ", "").strip()
    for pb in PB_BRANDS:
        if pb in clean_brand: return "PB"
    key = str(cat1).strip() + str(cat2).strip()
    return CATEGORY_MAP.get(key, "기타")

def load_weight_file_to_dict(file_name):
    if file_name in LOADED_WEIGHTS_MAP: return LOADED_WEIGHTS_MAP[file_name]
    paths = [os.path.join(WEIGHT_FOLDER, file_name), file_name]
    f_path = next((p for p in paths if os.path.exists(p)), None)
    if not f_path: return None
    try:
        try: df = pd.read_csv(f_path, encoding='utf-8-sig')
        except: df = pd.read_csv(f_path, encoding='cp949')
        df.columns = [c.strip().lower() for c in df.columns]
        if df['weight'].dtype == object:
            df['weight'] = df['weight'].astype(str).str.replace('%', '')
            df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
            if df['weight'].mean() > 5: df['weight'] /= 100
        df = df.groupby(['date', 'hour'])['weight'].mean().reset_index()
        w_map = dict(zip(zip(df['date'], df['hour']), df['weight']))
        LOADED_WEIGHTS_MAP[file_name] = w_map
        return w_map
    except: return None

def calc_final_weighted_mins(target_date, b_time, simple_mins, channel):
    if simple_mins <= 0: return 0
    year = target_date.year
    f_name = "weight_2022_2025.csv" if 2022 <= year <= 2025 else f"weight_{target_date.strftime('%Y%m')}.csv"
    w_map = load_weight_file_to_dict(f_name)
    if w_map is None: return "n/a"
    try:
        start_hour = int(b_time.split(':')[0])
        d_str = target_date.strftime("%Y-%m-%d")
        csv_rate = w_map.get((d_str, start_hour))
        if csv_rate is None: return "n/a"
    except: return "n/a"
    ch_rate = 0.7 if channel == "IPTV" else (0.3 if channel == "CATV" else 1.0)
    return round(simple_mins * csv_rate * ch_rate, 2)

def calc_duration_minutes(time_str):
    if not time_str or "~" not in time_str: return 0
    try:
        s, e = time_str.split("~")
        fmt = "%H:%M"
        ts = datetime.strptime(s.strip(), fmt)
        te = datetime.strptime(e.strip(), fmt)
        if te < ts: te += timedelta(days=1)
        return int((te - ts).total_seconds() / 60)
    except: return 0

# ==========================================
# [메인] 실행 로직 (안전하게 3일치 수집)
# ==========================================
def run():
    # 1. 한국 시간(KST) 기준 날짜 계산
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today = kst_now.date()
    
    # [수정] 누락 방지를 위해 [그저께 ~ 오늘] 3일치를 수집 범위로 설정
    start_date = today - timedelta(days=2) 
    end_date = today
    
    print(f"🚀 [KST] 수집 범위: {start_date} ~ {end_date} (자동 복구 모드)")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "X-Requested-With": "XMLHttpRequest"})
    
    new_rows = []
    
    # 3일간 루프 돌며 수집
    delta = end_date - start_date
    for i in range(delta.days + 1):
        target_date = start_date + timedelta(days=i)
        p_date = target_date.strftime("%Y/%m/%d")
        s_date = target_date.strftime("%Y-%m-%d")
        
        print(f"   📅 {s_date} 데이터 확인 중...", end="")
        try:
            url = "https://www.shinsegaetvshopping.com/broadcast/tvschedule-ajax"
            resp = session.get(url, params={"fromDate": p_date, "tomorrowYn": "N", "_": int(time.time()*1000)}, timeout=10, verify=False)
            soup = BeautifulSoup(resp.text, "html.parser")
            dl_list = soup.select("dl")
            
            cnt = 0
            if dl_list:
                times = [dl.select_one("dt > span._time").get_text(strip=True) for dl in dl_list if dl.select_one("dt > span._time")]
                t_cnt = Counter(times)
                t_seen = {}
                
                for dl in dl_list:
                    tt = dl.select_one("dt > span._time")
                    bt = tt.get_text(strip=True) if tt else ""
                    freq = t_cnt[bt]
                    ch = "전체"
                    if freq > 1:
                        seen = t_seen.get(bt, 0)
                        ch = "IPTV" if seen == 0 else "CATV"
                        t_seen[bt] = seen + 1
                    
                    sm = calc_duration_minutes(bt)
                    wm = calc_final_weighted_mins(target_date, bt, sm, ch)
                    
                    cards = dl.select("dd > div.card[data-main='Y']")
                    for card in cards:
                        try:
                            full_cat = card.get("data-gtm-item-category", "")
                            cats = full_cat.split(">") + [""]*5
                            c1, c2, c3, c4, c5 = cats[:5]
                            brand = card.get("data-gtm-item-brand", "").split('(')[0].strip()
                            name = card.get("data-gtm-item-name", "").strip()
                            pd_val = card.get("data-gtm-item-discount", "0")
                            price = int(str(pd_val).replace(",", "")) if pd_val else 0
                            gid = card.get("data-gtm-item-id", "")
                            link = f"https://www.shinsegaetvshopping.com/display/detail/{gid}" if gid else ""
                            img = card.select_one("img")
                            i_url = img.get("src", "").replace("_wg_", "_s_") if img else ""
                            if i_url.startswith("//"): i_url = "https:" + i_url
                            promo = card.select_one("._promoCharge")
                            p_txt = promo.get_text(strip=True) if promo else ""
                            md_class = determine_md_class(brand, c1, c2)

                            new_rows.append({
                                "방송일자": s_date, "방송시간": bt, "채널구분": ch, "단순분": sm, "가중분": wm,
                                "아이템분류1": c1, "아이템분류2": c2, "아이템분류3": c3, "아이템분류4": c4, "아이템분류5": c5,
                                "브랜드": brand, "상품명": name, "판매가": price, "할인가": price, "프로모션": p_txt,
                                "상품ID": gid, "이미지URL": i_url, "상세링크": link, "MD분류": md_class
                            })
                            cnt += 1
                        except: continue
            print(f" ✅ {cnt}건 수집")
        except Exception as e:
            print(f" ❌ 에러: {e}")

    # 3. 데이터 병합 (수집된 3일치 구간을 덮어쓰기)
    headers = [
        "방송일자", "방송시간", "채널구분", "단순분", "가중분",
        "아이템분류1", "아이템분류2", "아이템분류3", "아이템분류4", "아이템분류5",
        "브랜드", "상품명", "판매가", "할인가", "프로모션", 
        "상품ID", "이미지URL", "상세링크", "MD분류"
    ]
    
    new_df = pd.DataFrame(new_rows, columns=headers)
    
    if os.path.exists(FILENAME):
        try:
            existing_df = pd.read_csv(FILENAME, encoding='utf-8-sig')
            
            # [수정] 수집한 기간(3일)에 해당하는 기존 데이터 삭제 (Overwrite)
            update_dates = [ (start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta.days + 1) ]
            existing_df = existing_df[~existing_df['방송일자'].isin(update_dates)]
            
            # 합치기
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
            final_df = final_df.sort_values(by=['방송일자', '방송시간'])
            
            print(f"💾 병합 완료 (총 {len(final_df)}건)")
        except Exception as e:
            print(f"⚠️ 병합 오류, 새로 생성: {e}")
            final_df = new_df
    else:
        final_df = new_df

    final_df.to_csv(FILENAME, index=False, encoding='utf-8-sig')
    print(f"🎉 저장 완료: {FILENAME}")

if __name__ == "__main__":
    run()
