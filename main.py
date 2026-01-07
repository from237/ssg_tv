import requests
from bs4 import BeautifulSoup
import csv
import time
from datetime import date, timedelta, datetime
import urllib3
from collections import Counter
import os
import pandas as pd

# 보안 경고 끄기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# [설정] 파일명 (GitHub Action과 통일)
# ==========================================
FILENAME = "data.csv"
WEIGHT_FOLDER = "weights"
LOADED_WEIGHTS_MAP = {}

# ==========================================
# [설정] MD 분류 및 카테고리 매핑
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
        if not {'date', 'hour', 'weight'}.issubset(df.columns): return None
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
# [핵심] 날짜 자동 계산 함수
# ==========================================
def get_date_range():
    today = date.today()
    # 기본값: 1월 5일부터 (만약 파일이 없거나 읽기 실패하면 이 날짜부터 시작)
    start_date = date(2026, 1, 5) 

    if os.path.exists(FILENAME):
        try:
            print(f"📂 {FILENAME} 읽는 중...")
            df = pd.read_csv(FILENAME, encoding='utf-8-sig')
            if not df.empty and '방송일자' in df.columns:
                last_date_str = df['방송일자'].max() # 가장 마지막 날짜 (예: 2026-01-04)
                last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
                
                # 마지막 데이터가 오늘보다 이전이면, 그 다음날부터 수집
                if last_date < today:
                    start_date = last_date + timedelta(days=1)
                    print(f"🔄 마지막 데이터: {last_date} -> {start_date}부터 수집 시작")
                else:
                    start_date = today # 이미 최신이면 오늘것만
                    print("✅ 이미 최신 데이터까지 있습니다. 오늘 날짜만 확인합니다.")
        except Exception as e:
            print(f"⚠️ 파일 읽기 실패 (기본값 1월 5일로 시작): {e}")

    return start_date, today

def run():
    start_date, end_date = get_date_range()
    print(f"🚀 수집 시작: {start_date} ~ {end_date}")

    file_exists = os.path.exists(FILENAME)
    # 파일이 있으면 'a'(추가), 없으면 'w'(새로쓰기)
    mode = 'a' if file_exists else 'w'
    
    headers = [
        "방송일자", "방송시간", "채널구분", "단순분", "가중분",
        "아이템분류1", "아이템분류2", "아이템분류3", "아이템분류4", "아이템분류5",
        "브랜드", "상품명", "판매가", "할인가", "프로모션",
        "상품ID", "이미지URL", "상세링크",
        "MD분류"
    ]

    with open(FILENAME, mode, newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(headers) # 헤더는 파일 처음 만들때만

        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0", "X-Requested-With": "XMLHttpRequest"})

        delta = (end_date - start_date).days

        for i in range(delta + 1):
            target_date = start_date + timedelta(days=i)
            p_date = target_date.strftime("%Y/%m/%d")
            s_date = target_date.strftime("%Y-%m-%d")
            print(f"[{i + 1}/{delta + 1}] 📅 {s_date}...", end="")

            try:
                url = "https://www.shinsegaetvshopping.com/broadcast/tvschedule-ajax"
                resp = session.get(url, params={"fromDate": p_date, "tomorrowYn": "N", "_": int(time.time() * 1000)},
                                   timeout=10, verify=False)
                soup = BeautifulSoup(resp.text, "html.parser")
                dl_list = soup.select("dl")

                day_data = []
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
                                cats = full_cat.split(">") + [""] * 5
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

                                day_data.append([
                                    s_date, bt, ch, sm, wm,
                                    c1, c2, c3, c4, c5,
                                    brand, name, price, price, p_txt,
                                    gid, i_url, link,
                                    md_class
                                ])
                            except: continue

                    if day_data:
                        writer.writerows(day_data)
                        print(f" ✅ {len(day_data)}건 저장")
                    else:
                        print(f" ⚠️ 데이터 없음")
                else:
                    print(f" ⚠️ 방송 정보 없음")
                
                time.sleep(1)
            except Exception as e:
                print(f" ❌ 에러: {e}")

    print(f"\n🎉 완료! ({FILENAME}에 업데이트 됨)")

if __name__ == "__main__":
    run()
