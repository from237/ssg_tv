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
import math
import subprocess  # Git 자동화를 위한 모듈

# 보안 경고 끄기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# [설정] 수집 날짜 (자동 분할 저장됨)
# ==========================================
START_DATE = date(2026, 5, 20)
END_DATE = date(2026, 5, 20)

WEIGHT_FOLDER = "weights"
MASTER_WEIGHT_FILE = "weight_2022_2025.csv"

LOADED_WEIGHTS_MAP = {}
LOADED_FALLBACK_MAP = {}
MASTER_FALLBACK_MAP = {}

# PB 브랜드 목록
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


def init_master_fallback():
    global MASTER_FALLBACK_MAP
    paths = [os.path.join(WEIGHT_FOLDER, MASTER_WEIGHT_FILE), MASTER_WEIGHT_FILE]
    f_path = next((p for p in paths if os.path.exists(p)), None)

    if not f_path:
        print(f"⚠️ 경고: 마스터 가중치 파일({MASTER_WEIGHT_FILE}) 없음 -> 없을 시 100% 적용")
        return

    try:
        try:
            df = pd.read_csv(f_path, encoding='utf-8-sig')
        except:
            df = pd.read_csv(f_path, encoding='cp949')

        df.columns = [c.strip().lower() for c in df.columns]

        if 'hour' not in df.columns:
            print("⚠️ 마스터 파일에 'hour' 컬럼이 없습니다. 가중치가 1.0으로 고정됩니다.")
            return

        df['hour'] = pd.to_numeric(df['hour'], errors='coerce').fillna(0).astype(int)

        # [수정됨] Pandas 버전에 상관없이 무조건 문자열 치환 후 숫자로 변환
        if 'weight' in df.columns:
            df['weight'] = df['weight'].astype(str).str.replace('%', '', regex=False).str.replace(',', '', regex=False)
            df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
            if df['weight'].mean() > 5: df['weight'] /= 100

        df['dt'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['dt', 'weight'])
        df['weekday'] = df['dt'].dt.weekday

        df_avg = df.groupby(['weekday', 'hour'])['weight'].mean().reset_index()
        MASTER_FALLBACK_MAP.clear()
        for _, row in df_avg.iterrows():
            MASTER_FALLBACK_MAP[(int(row['weekday']), int(row['hour']))] = float(row['weight'])

        print(f"✅ 마스터 가중치 로딩 완료 (총 {len(MASTER_FALLBACK_MAP)}개 시간대)")

    except Exception as e:
        print(f"❌ 마스터 파일 로딩 에러: {e}")


def load_weight_file_to_dict(file_name):
    if file_name in LOADED_WEIGHTS_MAP: return LOADED_WEIGHTS_MAP[file_name]

    paths = [os.path.join(WEIGHT_FOLDER, file_name), file_name]
    f_path = next((p for p in paths if os.path.exists(p)), None)
    if not f_path: return None

    try:
        try:
            df = pd.read_csv(f_path, encoding='utf-8-sig')
        except:
            df = pd.read_csv(f_path, encoding='cp949')

        df.columns = [c.strip().lower() for c in df.columns]
        if not {'date', 'hour', 'weight'}.issubset(df.columns): return None

        df['hour'] = pd.to_numeric(df['hour'], errors='coerce').fillna(0).astype(int)

        # [수정됨] Pandas 버전에 상관없이 무조건 문자열 치환 후 숫자로 변환
        if 'weight' in df.columns:
            df['weight'] = df['weight'].astype(str).str.replace('%', '', regex=False).str.replace(',', '', regex=False)
            df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
            if df['weight'].mean() > 5: df['weight'] /= 100

        df['dt'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['dt', 'weight'])
        df['weekday'] = df['dt'].dt.weekday
        df['date_str'] = df['dt'].dt.strftime("%Y-%m-%d")

        df_exact = df.groupby(['date_str', 'hour'])['weight'].mean().reset_index()
        w_map = {}
        for _, row in df_exact.iterrows():
            w_map[(row['date_str'], int(row['hour']))] = float(row['weight'])
        LOADED_WEIGHTS_MAP[file_name] = w_map

        df_fallback = df.groupby(['weekday', 'hour'])['weight'].mean().reset_index()
        f_map = {}
        for _, row in df_fallback.iterrows():
            f_map[(int(row['weekday']), int(row['hour']))] = float(row['weight'])
        LOADED_FALLBACK_MAP[file_name] = f_map

        return w_map
    except Exception as e:
        print(f"❌ 개별 가중치 파일({file_name}) 로딩 에러: {e}")
        return None


def calc_final_weighted_mins(target_date, b_time, simple_mins, channel):
    if simple_mins <= 0: return 0

    year = target_date.year

    if 2022 <= year <= 2025:
        f_name = MASTER_WEIGHT_FILE
    else:
        f_name = f"weight_{target_date.strftime('%Y%m')}.csv"

    csv_rate = None
    w_map = load_weight_file_to_dict(f_name)

    start_hour = int(b_time.split(':')[0])
    weekday = int(target_date.weekday())

    if w_map:
        d_str = target_date.strftime("%Y-%m-%d")
        csv_rate = w_map.get((d_str, start_hour))
        if csv_rate is None:
            fallback_map = LOADED_FALLBACK_MAP.get(f_name, {})
            csv_rate = fallback_map.get((weekday, start_hour))

    if csv_rate is None:
        csv_rate = MASTER_FALLBACK_MAP.get((weekday, start_hour))

    if csv_rate is None:
        csv_rate = 1.0

    ch_rate = 0.7 if channel == "IPTV" else (0.3 if channel == "CATV" else 1.0)

    base_weighted_mins = simple_mins * csv_rate * ch_rate
    up_weighted_mins = base_weighted_mins * 1.09

    return int(math.ceil(up_weighted_mins))


def calc_duration_minutes(time_str):
    if not time_str or "~" not in time_str: return 0
    try:
        s, e = time_str.split("~")
        fmt = "%H:%M"
        ts = datetime.strptime(s.strip(), fmt)
        te = datetime.strptime(e.strip(), fmt)
        if te < ts: te += timedelta(days=1)
        return int((te - ts).total_seconds() / 60)
    except:
        return 0


def push_to_github():
    try:
        print("\n🐙 [Git] 변경 사항을 GitHub에 푸시합니다...")

        subprocess.run(["git", "add", "."], check=True)

        try:
            today_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            commit_message = f"Data Update: {today_str}"
            subprocess.run(["git", "commit", "-m", commit_message], check=False)
        except:
            pass

        subprocess.run(["git", "push", "-f", "origin", "main"], check=True)
        print(" 🚀 GitHub 푸시 성공! (Force Push)")

    except subprocess.CalledProcessError as e:
        print(f" ❌ Git 오류: {e}")
    except Exception as e:
        print(f" ❌ 시스템 오류: {e}")


def run():
    print(f"🚀 [자동 분할 모드] 수집 시작: {START_DATE} ~ {END_DATE}")

    init_master_fallback()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Referer": "https://m.shinsegaetvshopping.com/broadcast/tvschedule",
        "X-Requested-With": "XMLHttpRequest",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://m.shinsegaetvshopping.com"
    })

    headers_list = [
        "방송일자", "방송시간", "채널구분", "단순분", "가중분",
        "아이템분류1", "아이템분류2", "아이템분류3", "아이템분류4", "아이템분류5",
        "브랜드", "상품명", "판매가", "할인가", "프로모션",
        "상품ID", "이미지URL", "상세링크",
        "MD분류"
    ]

    delta = (END_DATE - START_DATE).days

    for i in range(delta + 1):
        target_date = START_DATE + timedelta(days=i)
        year = target_date.year
        current_filename = f"{year}data.csv"

        p_date = target_date.strftime("%Y/%m/%d")
        s_date = target_date.strftime("%Y-%m-%d")
        print(f"[{i + 1}] 📅 {s_date} -> 📂 {current_filename} 저장 중...", end="")

        try:
            daily_rows = []
            session.cookies.clear()
            url = "https://www.shinsegaetvshopping.com/broadcast/tvschedule-ajax"
            params = {"fromDate": p_date, "tomorrowYn": "N", "_": int(time.time() * 1000)}

            resp = session.get(url, params=params, timeout=20, verify=False)

            if resp.status_code != 200:
                print(f" ❌ 차단됨 ({resp.status_code})")
                time.sleep(3)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            dl_list = soup.select("dl")

            if dl_list:
                times = [dl.select_one("dt > span._time").get_text(strip=True) for dl in dl_list if
                         dl.select_one("dt > span._time")]
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

                            daily_rows.append([
                                s_date, bt, ch, sm, wm,
                                c1, c2, c3, c4, c5,
                                brand, name, price, price, p_txt,
                                gid, i_url, link,
                                md_class
                            ])
                        except:
                            continue

            if daily_rows:
                df_new = pd.DataFrame(daily_rows, columns=headers_list)

                if os.path.exists(current_filename):
                    try:
                        df_old = pd.read_csv(current_filename, encoding='utf-8-sig')
                    except Exception:
                        df_old = pd.DataFrame(columns=headers_list)

                    df_combined = pd.concat([df_old, df_new], ignore_index=True)
                    df_combined.drop_duplicates(subset=['방송일자', '방송시간', '상품ID'], keep='last', inplace=True)
                    df_combined.to_csv(current_filename, index=False, encoding='utf-8-sig')
                else:
                    df_new.to_csv(current_filename, index=False, encoding='utf-8-sig')

                print(f" ✅ {len(daily_rows)}건 수집 / 저장 완료")
            else:
                print(f" ⚠️ 데이터 없음")

            time.sleep(random.uniform(2, 5))

        except Exception as e:
            print(f" ❌ 에러: {e}")

    print(f"\n🎉 모든 수집 및 파일 저장이 완료되었습니다!")

    push_to_github()


if __name__ == "__main__":
    run()