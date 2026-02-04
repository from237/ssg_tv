from curl_cffi import requests
import json
import time
import random
import csv
import sys
from datetime import datetime, timedelta
import urllib3

# SSL 경고 끄기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==============================================================================
# [설정 1] 사장님 쿠키 (원본 그대로 사용)
# ==============================================================================
# 줄바꿈이나 공백이 있어도 그대로 둡니다. (서버가 검증하는 값일 수 있음)
RAW_COOKIE = """
_fwb=913B05Y0udlaj7fj4ivE7s.1753230912670; _ga=GA1.1.230261802.1753230914; sales2=eyJoaXN0b3J5IjpbNTAwMDAwMDMsImY0ZDZmMTI4YjI2MmQ2Mjk4Y2RiZDQ1N2ZlN2Y2YWYyIiwiMzMzOWIxZWU4Nzk0MDE2Y2RkMjY2MjNkYzYxMTE3MDMiXSwicGFzdF9rZXl3b3JkMiI6IuuniOydtO2UjOuhnOyasCIsImxhYmFuZ19vYmoiOnsiMzMzOWIxZWU4Nzk0MDE2Y2RkMjY2MjNkYzYxMTE3MDMiOiLsgrzshLHsoITsnpAg6rCk65+t7IucIOuLpO2SiOuqqSDruYXsiqTrp4jsnbzrjbDsnbQg7Yq56rCAISJ9LCJ1c2VyIjp7InVzZXJfaWQiOiJIQXdqLWN1OWlxWFZVblJPeDFPOEoiLCJuaWNrbmFtZSI6Iuu5hOuwgOuCmOq3uOuEpCIsInNlc3NfaWQiOiJ1MFRMWExZZW4yTXREMFNXX0JJdUUiLCJ1c2VyX3R5cGUiOjAsInZvdWNoZXIiOjAsInByZWZlciI6MSwicGxhbiI6eyJzZXJ2aWNlX2lkIjoiYml6X3YwLXkiLCJzdGF0dXMiOjEsInN0YXJ0X3RpbWVzdGFtcCI6IjIwMjYtMDEtMTVUMDQ6MDY6MTcuMDAwWiJ9fX0=; sales2.sig=JxyYyjEgS4FL_Of-qqTZg4GQ1SA; _ga_VN7F3DELDK=GS2.1.s1770084101$o51$g1$t1770084988$j60$l0$h0; _ga_NLGYGNTN3F=GS2.1.s1770084102$o51$g1$t1770084988$j60$l0$h0
"""

# ==============================================================================
# [설정 2] 수집 기간 (2025-11-01 ~ 2026-02-03)
# ==============================================================================
FILTER_SETTINGS = {
    "date_start": "20251101",
    "date_end": "20260203",
}


def clean_cookie_minimal(raw):
    # 최소한의 줄바꿈만 제거하고 내용은 건드리지 않음
    return raw.replace("\n", "").replace("\r", "").strip()


def get_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Cookie": clean_cookie_minimal(RAW_COOKIE),  # 원본 쿠키 탑재
        "Referer": "https://live.ecomm-data.com/",
        "Origin": "https://live.ecomm-data.com",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin"
    }


def run_api_crawler():
    final_rows = []

    start_date = datetime.strptime(FILTER_SETTINGS['date_start'], "%Y%m%d")
    end_date = datetime.strptime(FILTER_SETTINGS['date_end'], "%Y%m%d")

    print(f"\n[*] V53: 편성표 API 정밀 타격 시작 ({FILTER_SETTINGS['date_start']} ~ {FILTER_SETTINGS['date_end']})")
    print("[*] 전략: 원본 쿠키(방화벽+인증) 전송으로 401 차단 우회\n")

    current_date = start_date
    total_days = (end_date - start_date).days + 1
    processed_days = 0

    api_url = "https://live.ecomm-data.com/search2/hsshow"

    while current_date <= end_date:
        target_date_str = current_date.strftime("%Y%m%d")

        # 날짜 범위 지정 (하루 전체)
        start_ts = f"{target_date_str}0000"
        end_ts = f"{target_date_str}2359"

        payload = {
            "curr_page": 1,
            "page_size": 2000,
            "direction": "asc",
            "sort_name": "broadcast_start_time",
            "during": [start_ts, end_ts]
        }

        try:
            # POST 요청
            res = requests.post(api_url, json=payload, headers=get_headers(), impersonate="chrome124", verify=False)

            if res.status_code == 200:
                data = res.json()
                items = data.get('list', [])

                day_count = 0
                for item in items:
                    sales_cnt = item.get('sales_cnt', 0) or 0
                    sales_amt = item.get('sales_amt', 0) or 0

                    # 날짜 포맷
                    raw_start = item.get('hsshow_datetime_start', '')
                    fmt_date = ""
                    if len(raw_start) >= 12:
                        fmt_date = f"{raw_start[:4]}-{raw_start[4:6]}-{raw_start[6:8]} {raw_start[8:10]}:{raw_start[10:12]}"

                    cat_info = item.get('cat', {})

                    row = [
                        item.get('hsshow_id'),
                        fmt_date,
                        item.get('platform_name'),
                        cat_info.get('cat_name', ''),
                        item.get('hsshow_title'),
                        sales_cnt,  # 판매량
                        sales_amt,  # 매출액
                        item.get('item_cnt', 0),
                        item.get('status', 0)
                    ]
                    final_rows.append(row)
                    day_count += 1

                processed_days += 1
                print(f"\r >> [{processed_days}/{total_days}] {target_date_str}: {day_count}건 수집 성공", end="")

            elif res.status_code == 401:
                print(f"\n\n[!!!] 401 에러: 쿠키가 거부되었습니다.")
                print("[팁] 이 에러가 계속되면 쿠키가 'IP 주소'에 종속된 것일 수 있습니다.")
                print("     (브라우저가 켜진 PC와 코드를 돌리는 PC가 같아야 합니다.)")
                break
            elif res.status_code == 402:
                print(f"\n\n[!!!] 402 에러: 유료 결제 필요 (일일 조회량 초과 등)")
                break
            else:
                print(f" [!] {target_date_str} 실패 (Code: {res.status_code})")

        except Exception as e:
            print(f" [!] 에러: {e}")

        # 다음 날짜로 이동
        current_date += timedelta(days=1)
        time.sleep(0.5)

    return final_rows


def save_csv(data_rows):
    if not data_rows:
        return

    filename = "방송데이터_매출완벽포함_V53.csv"
    data_rows.sort(key=lambda x: x[1])

    with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
        wr = csv.writer(f)
        header = [
            '방송ID', '방송일시', '방송사', '카테고리',
            '상품명', '판매량', '매출액', '상품수', '상태'
        ]
        wr.writerow(header)
        wr.writerows(data_rows)
    print(f"\n\n[✨ 저장 완료] '{filename}' 파일 확인 (총 {len(data_rows)}건)")


if __name__ == "__main__":
    data = run_api_crawler()
    if data:
        save_csv(data)
    else:
        print("\n[!] 데이터 수집 실패.")