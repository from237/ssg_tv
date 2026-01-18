import requests
import pandas as pd
import urllib3

# SSL 경고 메시지 제거
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. API 주소
url = "https://api.queenit.kr/general-products"

# 2. 헤더 설정 (최신 인증키 적용됨)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Origin": "https://web.queenit.kr",
    "Referer": "https://web.queenit.kr/",

    # 인증키 (만료 시 브라우저에서 새로 복사 필요)
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJlMGRjMTUwOS0zNDk3LTQyNTItODk5Mi1lZDgxYmY0OTg2ZTciLCJpc3MiOiJkYW1vYS5yYXBwb3J0bGFicy5rcjp2MSIsImlhdCI6MTc2ODU0NjgyNCwiZXhwIjoxNzY4NTUwNDI0LCJ0eXBlIjoiQUNDRVNTIiwidWlkIjoidWlkXzg5ZjNjN2JlNjk2YjllOGZlYzYwODcxNzVhZjVlMTdhIiwidXNlclJvbGUiOiJBTk9OWU1PVVMiLCJpc1Rlc3RlciI6ZmFsc2V9.tswblo1wA7WlzeJ0sivMqmYH-_N3K8XlYZXQUW0l-qw"
}

# 3. Payload 설정 (요청 조건)
# ==========================================
# [참고] 카테고리 ID 목록 (필요시 복사해서 categoryIds에 넣으세요)
# 골프 : f2vinx
# 리빙 : fg5pex
# 스포츠 : ikyytr
# 남성 : dxbp1m
# 뷰티 : zdwhc2
# 여성 : kkqk6l
# ==========================================

payload = {
    "categoryIds": ["kkqk6l"],  # 현재 설정: 여성 (kkqk6l)
    "size": 500,  # 👈 요청하신 대로 500개로 변경 완료!
    "orderBy": "RECOMMENDATION",
    "categoryGenders": ["FEMALE", "UNISEX", "NONE"],
    "brandIds": [],
    "compoundedTagIdsForProductTag": [],
    "productIdsInLandingUrl": [],
    "listId": "home-brandPromotion",
    "context": {}
}

# 4. 실행 및 저장
try:
    print(f"🚀 '{payload['categoryIds'][0]}' 카테고리에서 상품 500개 요청 중...")
    response = requests.post(url, headers=headers, json=payload, verify=False)

    if response.status_code == 200:
        data = response.json()

        product_list = []
        items = data.get('items') or data.get('list') or data.get('set') or []

        print(f"📦 총 {len(items)}개의 상품 데이터를 받았습니다.")

        for item in items:
            # 브랜드명 추출
            brand_name = item.get("brand")
            if isinstance(brand_name, dict):
                brand_name = brand_name.get("name")

            # 상품 속성(태그) 추출
            tags_list = item.get("tags", {}).get("list", [])
            tag_names = [tag.get("name") for tag in tags_list if tag.get("name")]
            attributes_str = ", ".join(tag_names)

            info = {
                "브랜드": brand_name,
                "상품명": item.get("name"),
                "상품속성": attributes_str,
                "판매가": item.get("finalPrice") or item.get("price"),
                "정가": item.get("originalPrice"),
                "할인율": item.get("discountPercentage") or item.get("discountRate"),
                "링크": f"https://web.queenit.kr/product/{item.get('productId') or item.get('id')}"
            }
            product_list.append(info)

        if product_list:
            file_name = "queenit_products_500.xlsx"
            df = pd.DataFrame(product_list)
            df.to_excel(file_name, index=False)
            print(f"✅ '{file_name}' 파일로 저장 완료!")
            print(df.head())
        else:
            print("⚠️ 데이터 리스트가 비어있습니다. (인증키 만료 가능성 있음)")

    else:
        print(f"❌ 요청 실패 (코드: {response.status_code})")
        print("이유:", response.text)

except Exception as e:
    print(f"❗ 에러: {e}")