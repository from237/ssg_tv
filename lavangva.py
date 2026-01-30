from curl_cffi import requests
from bs4 import BeautifulSoup
import json
import time
import random
import csv
import sys

# ==============================================================================
# [설정 1] 쿠키 값 (사장님 최신 쿠키 적용)
# ==============================================================================
RAW_COOKIE = """
_fwb=913B05Y0udlaj7fj4ivE7s.1753230912670; _ga=GA1.1.230261802.1753230914; sales2=eyJoaXN0b3J5IjpbImY0ZDZmMTI4YjI2MmQ2Mjk4Y2RiZDQ1N2ZlN2Y2YWYyIiw1MDAwMDAwMywiMzMzOWIxZWU4Nzk0MDE2Y2RkMjY2MjNkYzYxMTE3MDMiXSwicGFzdF9rZXl3b3JkMiI6IuuniOydtO2UjOuhnOyasCIsImxhYmFuZ19vYmoiOnsiMzMzOWIxZWU4Nzk0MDE2Y2RkMjY2MjNkYzYxMTE3MDMiOiLsgrzshLHsoITsnpAg6rCk65+t7IucIOuLpO2SiOuqqSDruYXsiqTrp4jsnbzrjbDsnbQg7Yq56rCAISJ9LCJ1c2VyIjp7InVzZXJfaWQiOiJIQXdqLWN1OWlxWFZVblJPeDFPOEoiLCJuaWNrbmFtZSI6Iuu5hOuwgOuCmOq3uOuEpCIsInNlc3NfaWQiOiJtM0FJV3lPUWE2UDlvckstZENZdXEiLCJ1c2VyX3R5cGUiOjAsInZvdWNoZXIiOjAsInByZWZlciI6MSwicGxhbiI6eyJzZXJ2aWNlX2lkIjoiYml6X3YwLXkiLCJzdGF0dXMiOjEsInN0YXJ0X3RpbWVzdGFtcCI6IjIwMjYtMDEtMTVUMDQ6MDY6MTcuMDAwWiJ9fX0=; sales2.sig=3cxelzCDT41v3iltspJXsjJa2po; _ga_VN7F3DELDK=GS2.1.s1769728629$o49$g1$t1769732829$j31$l0$h0; _ga_NLGYGNTN3F=GS2.1.s1769728629$o49$g1$t1769732829$j31$l0$h0
"""

# ==============================================================================
# [설정 2] 기간 및 카테고리
# ==============================================================================
FILTER_SETTINGS = {
    "date_start": "20251101",
    "date_end": "20260129",
}

# 전체 카테고리 코드
TARGET_CATEGORIES = {
    "50005542": "도서", "50000010": "면세점", "50000000": "패션의류",
    "50000001": "패션잡화", "50000002": "화장품/미용", "50000003": "디지털/가전",
    "50000004": "가구/인테리어", "50000005": "출산/육아", "50000006": "식품",
    "50000007": "스포츠/레저", "50000008": "생활건강"
}


def clean_cookie(raw):
    return raw.replace("\n", "").replace("\r", "").strip()


def get_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Cookie": clean_cookie(RAW_COOKIE),
        "Referer": "https://live.ecomm-data.com/",
    }


# 1. 초기화 (Build ID 따기 - API 아님)
def get_build_id():
    url = "https://live.ecomm-data.com/report/category/hs/50000003"
    print("[*] 1단계: 보안 우회를 위한 Build ID 획득 중...")
    try:
        # curl_cffi로 브라우저인 척 접속
        res = requests.get(url, headers=get_headers(), impersonate="chrome124")
        if res.status_code != 200:
            print(f"[-] 접속 실패 ({res.status_code})")
            return None

        soup = BeautifulSoup(res.text, 'html.parser')
        json_data = json.loads(soup.find('script', id='__NEXT_DATA__').string)
        build_id = json_data.get('buildId')
        print(f"[+] Build ID 확보 완료: {build_id}")
        return build_id
    except Exception as e:
        print(f"[-] 초기화 에러: {e}")
        return None


# 2. 크롤링 (API(402) 대신 Next.js 데이터 사용)
def run_crawler(build_id):
    final_rows = []
    print(f"\n[*] 2단계: 데이터 수집 시작 (API 미사용, 과금 우회)\n")

    for idx, (cat_id, cat_name) in enumerate(TARGET_CATEGORIES.items()):
        current_page = 1
        cat_count = 0

        while True:
            # 402 에러가 안 뜨는 '웹사이트 데이터 파일' 주소
            next_url = f"https://live.ecomm-data.com/_next/data/{build_id}/report/category/hs/{cat_id}.json"

            params = {
                'id': cat_id,
                'date_start': FILTER_SETTINGS['date_start'],
                'date_end': FILTER_SETTINGS['date_end'],
                'page': current_page,  # 페이지 번호 자동 증가
                'size': 100  # 한 페이지당 개수
            }

            try:
                res = requests.get(next_url, params=params, headers=get_headers(), impersonate="chrome124")

                if res.status_code == 200:
                    data = res.json().get('pageProps', {})
                    # 방송 목록 추출
                    shows = data.get('_relHsshows', {}).get('list', [])

                    if not shows:
                        # 더 이상 데이터가 없으면 다음 카테고리로
                        break

                    for show in shows:
                        # 날짜 필터링
                        raw_dt = show.get('hsshow_datetime_start', '')
                        if not raw_dt: continue
                        raw_date = raw_dt[:8]

                        # 기간 체크
                        if not (FILTER_SETTINGS['date_start'] <= raw_date <= FILTER_SETTINGS['date_end']):
                            continue

                        fmt_dt = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]} {raw_dt[8:10]}:{raw_dt[10:12]}"

                        # [중요] Next.js 데이터 구조에 맞춰 필드 매핑
                        # (API와 키 값이 다를 수 있어 안전하게 get 사용)
                        row = [
                            show.get('item_id', show.get('hsshow_id', '')),  # item_id가 없으면 show_id라도
                            show.get('item_thumbnail_url', show.get('hsshow_thumbnail_url', '')),
                            show.get('price', 0),
                            show.get('item_url', ''),
                            show.get('price_sales', 0),
                            show.get('sales_cnt', 0),
                            show.get('sales_amt', 0),
                            show.get('live_price', 0),
                            show.get('cid', cat_id),
                            show.get('item_name', show.get('hsshow_title', '')),  # 아이템명 없으면 방송제목
                            show.get('platform_name', ''),
                            show.get('cat', {}).get('cat_name', cat_name),  # 대분류
                            show.get('cid_name', ''),  # 세부카테고리
                            fmt_dt
                        ]
                        final_rows.append(row)
                        cat_count += 1

                    print(
                        f"\r[{idx + 1}/{len(TARGET_CATEGORIES)}] {cat_name}: {current_page}페이지 수집 중.. (누적 {cat_count}개)",
                        end="")
                    current_page += 1
                    time.sleep(0.2)  # 차단 방지 딜레이

                else:
                    # 404가 뜨면 페이지 끝
                    break

            except Exception as e:
                print(f"\n[-] 에러 발생: {e}")
                break

        print("")  # 줄바꿈

    return final_rows


def save_csv(data_rows):
    if not data_rows:
        return

    filename = "방송데이터_우회수집_완료.csv"
    data_rows.sort(key=lambda x: x[13])  # 날짜순 정렬

    with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
        wr = csv.writer(f)
        header = [
            'item_id', '썸네일URL', '정상가', '상품URL', '판매가', '판매량',
            '매출액', '할인가', '카테고리ID', '아이템명(방송명)',
            '회사명', '대분류', '세부카테고리', '방송일시'
        ]
        wr.writerow(header)
        wr.writerows(data_rows)
    print(f"\n[✨ 저장 완료] '{filename}' 파일 생성 완료! (총 {len(data_rows)}건)")


if __name__ == "__main__":
    b_id = get_build_id()
    if b_id:
        data = run_crawler(b_id)
        if data:
            save_csv(data)
        else:
            print("\n[!] 수집된 데이터가 없습니다. (기간 내 데이터 없음)")
    else:
        print("[!] 초기화 실패.")