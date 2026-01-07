import pandas as pd
from datetime import datetime
import time
import random
import os

# 파일명 고정
CSV_FILENAME = "data.csv"

# ==========================================
# 1. 수집 함수 (기존 로직 유지)
# ==========================================
def fetch_schedule_by_date(target_date_str):
    print(f"[{target_date_str}] 데이터 수집 시작...")
    
    # ---------------------------------------------------------
    # [Start] 실제 크롤링 로직 (여기에 작성)
    # ---------------------------------------------------------
    
    # (테스트용 더미 데이터 - 실제 적용 시 삭제하세요)
    dummy_data = []
    # 예: 5일, 6일 데이터가 들어온다고 가정
    for i in range(1, 4):
        dummy_data.append({
            'broadcast_date': target_date_str, # 날짜 (YYYY-MM-DD 권장)
            'start_time': f'{10+i}:00',        
            'program_name': f'방송상품 {target_date_str} - {i}', 
            'channel': 'SK Stoa',             
            'price': 59900
        })
    # ---------------------------------------------------------
    
    return dummy_data

# ==========================================
# 2. 메인 실행 로직 (Append & Deduplicate)
# ==========================================
def main():
    # -----------------------------------------------------------
    # [날짜 설정] 
    # 지금은 1월 5일, 6일만 필요하므로 아래 리스트를 사용합니다.
    # -----------------------------------------------------------
    target_dates = [
        datetime(2026, 1, 5).date(),
        datetime(2026, 1, 6).date()
    ]
    
    # [중요] 나중에 매일 1회 실행할 때는 위 코드를 지우고 아래 코드를 주석 해제해서 쓰세요.
    # target_dates = [datetime.now().date()]

    print(f"=========================================================")
    print(f"수집 대상 날짜 : {target_dates}")
    print(f"저장 파일명    : {CSV_FILENAME}")
    print(f"=========================================================\n")

    collected_data = []

    # 1. 데이터 수집 루프
    for current_date in target_dates:
        query_date_str = current_date.strftime('%Y-%m-%d') # 포맷 수정 가능
        
        try:
            daily_data = fetch_schedule_by_date(query_date_str)
            if daily_data:
                collected_data.extend(daily_data)
                print(f" >> 성공: {query_date_str} (데이터 {len(daily_data)}건)")
            else:
                print(f" >> 정보 없음: {query_date_str}")
        except Exception as e:
            print(f" >> 에러: {query_date_str} - {e}")
        
        time.sleep(random.uniform(0.5, 1.0))

    # 2. 파일 저장 (Append 방식)
    if collected_data:
        new_df = pd.DataFrame(collected_data)

        # 기존 파일이 있으면 불러와서 합치기 (중복 방지 및 단순 추가)
        if os.path.exists(CSV_FILENAME):
            print(f"\n기존 {CSV_FILENAME} 파일이 존재하여 데이터를 병합합니다.")
            existing_df = pd.read_csv(CSV_FILENAME)
            
            # 기존 데이터 + 새 데이터 합치기
            final_df = pd.concat([existing_df, new_df])
            
            # (옵션) 중복 제거: 날짜, 시간, 채널이 같으면 중복으로 보고 제거
            # 필요 없다면 아래 줄 주석 처리
            # final_df = final_df.drop_duplicates(subset=['broadcast_date', 'start_time', 'channel'], keep='last')
        else:
            print(f"\n새로운 {CSV_FILENAME} 파일을 생성합니다.")
            final_df = new_df

        # 저장
        final_df.to_csv(CSV_FILENAME, index=False, encoding='utf-8-sig')
        print(f"[완료] 총 {len(final_df)}건 저장 완료.")
        print(final_df.tail()) # 끝부분 데이터 확인

    else:
        print("\n[알림] 수집된 데이터가 없어 저장하지 않았습니다.")

if __name__ == "__main__":
    main()
