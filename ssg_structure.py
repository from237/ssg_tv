import requests
from bs4 import BeautifulSoup
import urllib3
import time
from datetime import date
import json

# 보안 경고 끄기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def inspect_shinsegae_structure():
    print("🕵️‍♀️ 신세계TV쇼핑 데이터 구조 정밀 분석을 시작합니다...\n")

    # 오늘 날짜 기준
    today_str = date.today().strftime("%Y/%m/%d")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.shinsegaetvshopping.com/broadcast/tvschedule",
        "X-Requested-With": "XMLHttpRequest"
    })

    url = "https://www.shinsegaetvshopping.com/broadcast/tvschedule-ajax"
    params = {"fromDate": today_str, "tomorrowYn": "N", "_": int(time.time() * 1000)}

    try:
        resp = session.get(url, params=params, timeout=10, verify=False)
        soup = BeautifulSoup(resp.text, "html.parser")

        # 가장 첫 번째 방송의 첫 번째 상품 카드를 가져옵니다.
        # (보통 첫 번째 데이터가 가장 잘 채워져 있을 확률이 높음)
        dl_list = soup.select("dl")

        target_card = None
        target_time = ""

        # 상품이 있는 시간대를 찾습니다.
        for dl in dl_list:
            cards = dl.select("dd > div.card")
            if cards:
                target_card = cards[0]
                target_time = dl.select_one("dt > span._time").get_text(strip=True)
                break

        if not target_card:
            print("❌ 현재 편성표에서 상품 정보를 찾을 수 없습니다.")
            return

        print(f"=== 🕒 방송 시간: {target_time} ===")
        print("=== 📦 첫 번째 상품 카드(HTML) 분석 결과 ===\n")

        # 1. 숨겨진 data- 속성 (여기에 알짜 정보가 많습니다)
        print("[1] 숨겨진 데이터 속성 (data-* attributes):")
        print(json.dumps(target_card.attrs, indent=4, ensure_ascii=False))
        print("-" * 50)

        # 2. 이미지 정보
        img_tag = target_card.select_one("img")
        print("\n[2] 이미지 태그 정보:")
        if img_tag:
            print(json.dumps(img_tag.attrs, indent=4, ensure_ascii=False))
        else:
            print("이미지 없음")
        print("-" * 50)

        # 3. 텍스트 정보 (화면에 보이는 모든 글자)
        print("\n[3] 화면에 표시된 텍스트 정보 (계층별):")

        # 브랜드
        brand = target_card.select_one(".goods_title .brand")
        if brand: print(f"- 브랜드: {brand.get_text(strip=True)}")

        # 상품명
        name = target_card.select_one(".goods_title .name")
        if name: print(f"- 상품명: {name.get_text(strip=True)}")

        # 가격
        price = target_card.select_one(".price_info .price strong")
        if price: print(f"- 판매가: {price.get_text(strip=True)}")

        # 할인전 가격 (있을 경우)
        old_price = target_card.select_one(".price_info .old_price")
        if old_price: print(f"- 정상가(할인전): {old_price.get_text(strip=True)}")

        # 혜택/프로모션 태그
        promos = target_card.select("._promoCharge")
        promo_texts = [p.get_text(strip=True) for p in promos]
        print(f"- 프로모션/혜택 텍스트: {promo_texts}")

        # 배지 (무료배송, 적립 등)
        badges = target_card.select(".benefit_list span")
        badge_texts = [b.get_text(strip=True) for b in badges]
        print(f"- 배지 목록: {badge_texts}")

        # 4. 링크 정보
        link = target_card.get("onclick")
        print(f"\n[4] 링크 동작 (onclick): {link}")

        # 5. [심화] 상세 페이지 URL 추정
        item_id = target_card.get("data-gtm-item-id")
        if item_id:
            detail_url = f"https://www.shinsegaetvshopping.com/display/detail/{item_id}"
            print(f"\n[5] 상품 상세 페이지 URL (추정): {detail_url}")
            print("   👉 원산지, 제조사 등 더 깊은 정보는 위 URL로 한번 더 요청(크롤링)해야 얻을 수 있습니다.")

    except Exception as e:
        print(f"에러 발생: {e}")


if __name__ == "__main__":
    inspect_shinsegae_structure()