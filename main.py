import pandas as pd
from datetime import datetime, timedelta
import time
import random

# ==========================================
# 1. 설정 및 수집 함수 (기존 로직 포함)
# ==========================================

def fetch_schedule_by_date(target_date_str):
    """
    특정 날짜의 편성표 데이터를 수집하는 함수 (Placeholder)
    실제 크롤링 로직(BeautifulSoup, API 호출 등)을 이곳에 배치하거나
    기존에 작성하신 코드를 이 안에 넣으시면 됩니다.
    """
    print(f"[{target_date_str}] 데이터 수집 시작...")
    
    # ---------------------------------------------------------
    # [Start] 실제 크롤링/API 로직이 들어갈 자리
    # 예시: response = requests.get(url + target_date_str) ...
    # ---------------------------------------------------------
    
    # (테스트를 위한 더미 데이터 생성 부분입니다. 실제 코드 적용 시 삭제하세요)
    dummy_data = []
    # 예: 하루에 방송 3개라고 가정
    for i in range(1, 4):
        dummy_data.append({
            'broadcast_date': target_date_str,
            'program_name': f'테스트 방송상품 {i}',
            'start_time': f'{10+i}:00',
            'price': 59900
        })
    
    # ---------------------------------------------------------
    # [End] 크롤링 로직 끝
    # ---------------------------------------------------------
    
    return dummy_data

# ==========================================
# 2. 메인 실행 로직 (날짜 범위 계산 및 루프)
# ==========================================

def main():
    # 1. 오늘 날짜 기준 설정
    today = datetime.now().date()
    
    # 2. 날짜 범위 설정: Today - 2일 ~ Today + 2일
    # - 과거 2일: 이미 지나갔지만 데이터 누락이나 사후 확정 정보를 위해 재수집 (1/4 이후 끊긴 데이터 연결)
    # - 미래 2일: 편성 정보 변경 가능성이 높으므로 최신본으로 갱신
    start_date = today - timedelta(days=2)
    end_date = today + timedelta(days=2)
    
    print(f"=========================================================")
    print(f"수집 기준일(Today): {today}")
    print(f"수집 타겟 구간    : {start_date} ~ {end_date} (총 5일간)")
    print(f"=========================================================\n")

    all_collected_data = []

    # 3. 날짜 루프 실행
    current_date = start_date
    while current_date <= end_date:
        # 날짜 포맷 변환 (예: '20260105') - 사이트 URL 구조에 맞춰 수정 가능
        query_date_str = current_date.strftime('%Y%m%d')
        
        try:
            # 수집 함수 호출
            daily_data = fetch_schedule_by_date(query_date_str)
            
            if daily_data:
                all_collected_data.extend(daily_data)
                print(f" >> 성공: {query_date_str} (데이터 {len(daily_data)}건)")
            else:
                print(f" >> 정보 없음: {query_date_str}")
                
        except Exception as e:
            print(f" >> 에러 발생: {query_date_str} - {e}")
        
        # 서버 부하 방지를 위한 딜레이 (필요 시 조절)
        time.sleep(random.uniform(0.5, 1.5))
        
        # 다음 날짜로 이동
        current_date += timedelta(days=1)

    # 4. 결과 저장 (Pandas DataFrame)
    if all_collected_data:
        df = pd.DataFrame(all_collected_data)
        
        # 중복 제거 로직 (선택 사항: 방송일시+상품명이 같으면 최신 것으로 덮어쓰기 등)
        # df = df.drop_duplicates(subset=['broadcast_date', 'program_name'])
        
        print(f"\n[완료] 총 {len(df)}건의 데이터가 수집되었습니다.")
        print(df.head())
        
        # 파일 저장 (덮어쓰기 모드 혹은 append 모드 선택)
        # 매일 5일치를 긁어오므로, 기존 파일과 병합(Merge)하는 로직이 필요할 수 있습니다.
        # 여기서는 일단 오늘 날짜로 된 CSV를 생성합니다.
        filename = f"schedule_data_{today.strftime('%Y%m%d')}.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"파일 저장 완료: {filename}")
        
    else:
        print("\n[알림] 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()
