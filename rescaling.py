import pandas as pd
import math
import os
import glob

# ==========================================
# [설정] 업데이트할 파일 패턴
# ==========================================
# 예: 2022data.csv, 2023data.csv 등 "연도data.csv" 패턴의 파일들
FILE_PATTERN = "*data.csv"


def update_csv_weights():
    # 현재 폴더에서 패턴에 맞는 파일 찾기
    files = glob.glob(FILE_PATTERN)

    if not files:
        print("⚠️ 업데이트할 CSV 파일(*data.csv)을 찾을 수 없습니다.")
        return

    print(f"🔍 총 {len(files)}개의 파일을 발견했습니다. 업데이트를 시작합니다...\n")

    for file_path in files:
        try:
            print(f"📂 [{file_path}] 처리 중...", end="")

            # CSV 파일 읽기 (인코딩 확인)
            try:
                df = pd.read_csv(file_path, encoding='utf-8-sig')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='cp949')

            # 컬럼 공백 제거 및 소문자 변환 (안전장치)
            # 실제 컬럼명은 유지하되, 비교를 위해 임시로 정리
            clean_cols = {c: c.strip() for c in df.columns}
            df.rename(columns=clean_cols, inplace=True)

            target_col = "가중분"

            if target_col not in df.columns:
                print(f" ❌ '{target_col}' 컬럼 없음 (스킵)")
                continue

            # [핵심 로직] 가중분 * 1.09 후 올림(ceil) 처리
            # 1. 기존 값에 1.09 곱하기
            # 2. math.ceil로 올림
            # 3. int로 변환
            # (NaN 값이나 숫자가 아닌 값이 있을 경우 0으로 처리하는 안전장치 포함)

            def apply_weight_up(x):
                try:
                    # 쉼표 제거 및 숫자 변환
                    val = float(str(x).replace(',', ''))
                    return int(math.ceil(val * 0.99))
                except:
                    return 0

            df[target_col] = df[target_col].apply(apply_weight_up)

            # 파일 덮어쓰기 (utf-8-sig)
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f" ✅ 업데이트 완료")

        except Exception as e:
            print(f" ❌ 에러 발생: {e}")

    print("\n🎉 모든 파일의 가중분 업데이트가 완료되었습니다!")


if __name__ == "__main__":
    update_csv_weights()