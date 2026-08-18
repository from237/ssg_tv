# -*- coding: utf-8 -*-
"""
run_all.py
==========
이 파일 하나만 실행하면 끝납니다.

  1) SK스토아  Redshift 조회 -> data/sk_{year}.csv 누적 + weight_curve.json 갱신
  2) SSG/K쇼핑 가중분을 SK 곡선 기준으로 재계산 -> data/ssg_*.csv, data/kshop_*.csv
  3) GitHub push

실행
----
  python run_all.py                          # 대화형 (직전 저장일 다음날부터)
  python run_all.py 2025-01-01 2026-08-17    # 기간 지정
  python run_all.py --no-push                # push 없이 파일만
  python run_all.py --rebuild-curve          # 가중치 곡선 강제 재산출
  python run_all.py --skip-sk                # SK 조회 건너뛰고 재계산만

⚠ push 전에 db_config.py 가 .gitignore에 걸려 있는지 반드시 확인합니다.
  안 걸려 있으면 push를 거부합니다.
"""

import os
import sys
import glob
import datetime
import subprocess
import traceback

CURVE_FILE = "weight_curve.json"
OUT_DIR = "data"


def banner(t):
    print("\n\n" + "█" * 78)
    print(f"█  {t}")
    print("█" * 78)


# ---------------------------------------------------------------------------
# git 안전장치
# ---------------------------------------------------------------------------
def git_guard():
    """db_config.py 가 정말 무시되는지 확인. 하나라도 걸리면 push 금지."""
    problems = []

    if not os.path.exists(".gitignore"):
        if os.path.exists("gitignore"):
            problems.append("'gitignore' 파일이 있는데 앞에 점이 없습니다. "
                            "'.gitignore' 로 이름을 바꾸세요:  ren gitignore .gitignore")
        else:
            problems.append(".gitignore 파일이 없습니다.")

    r = subprocess.run(["git", "check-ignore", "db_config.py"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        problems.append("db_config.py 가 .gitignore 규칙에 안 걸립니다.")

    r = subprocess.run(["git", "ls-files", "--error-unmatch", "db_config.py"],
                       capture_output=True, text=True)
    if r.returncode == 0:
        problems.append("db_config.py 가 이미 git에 추적되고 있습니다. "
                        "git rm --cached db_config.py 로 먼저 빼세요. "
                        "(과거 커밋에 남아 있으면 Redshift 비밀번호 교체 권장)")
    return problems


def push():
    banner("3단계 · GitHub push")

    problems = git_guard()
    if problems:
        print("   ❌ 접속정보 보호 상태가 확인되지 않아 push를 중단합니다.\n")
        for p in problems:
            print(f"      · {p}")
        print("\n   해결 후 다시 실행하거나, 파일만 만들려면 --no-push 를 쓰세요.")
        return False

    print("   ✅ db_config.py 보호 확인")
    try:
        targets = [OUT_DIR] + [f for f in (CURVE_FILE, "index.html") if os.path.exists(f)]
        subprocess.run(["git", "add"] + targets, check=True)

        st = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not st.stdout.strip():
            print("   변경 사항이 없습니다.")
            return True

        msg = f"Data update: {datetime.datetime.now():%Y-%m-%d %H:%M}"
        subprocess.run(["git", "commit", "-m", msg], check=True)
        subprocess.run(["git", "push", "origin", "main"], check=True)
        print("   🚀 push 완료")
        return True
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Git 오류: {e}")
        print("      원격 저장소/브랜치명을 확인하세요:  git remote -v")
        return False


# ---------------------------------------------------------------------------
def main():
    t0 = datetime.datetime.now()
    flags = [a for a in sys.argv[1:] if a.startswith("--")]
    args = [a for a in sys.argv[1:] if not a.startswith("--")]

    do_push = "--no-push" not in flags
    skip_sk = "--skip-sk" in flags

    print(f"실행 시각: {t0:%Y-%m-%d %H:%M:%S}")
    print(f"작업 폴더: {os.getcwd()}")

    # --- 1단계 ---
    if not skip_sk:
        banner("1단계 · SK스토아 Redshift 조회")
        import sk_extract as SK

        start, end = (args[0], args[1]) if len(args) >= 2 else SK.ask_date_range()

        raw = SK.fetch(start, end)
        if raw.empty:
            print("   ⚠ 해당 기간에 데이터가 없습니다. 재계산만 진행합니다.")
        else:
            df = SK.build_slots(raw)
            SK.ensure_curve(df, start, end, "--rebuild-curve" in flags)
            SK.merge_and_save(SK.to_rows(df, end), start, end)
            SK.validate_accumulated()
            SK.report_unmapped()
            SK.report_reclass()
    else:
        banner("1단계 · SK 조회 건너뜀 (--skip-sk)")

    if not os.path.exists(CURVE_FILE):
        sys.exit(f"\n❌ {CURVE_FILE} 이 없습니다. --skip-sk 없이 한 번 실행하세요.")

    # --- 2단계 ---
    banner("2단계 · SSG / K쇼핑 재계산")
    import others_rebuild
    others_rebuild.run()

    # --- 3단계 ---
    ok = push() if do_push else (print("\n(--no-push: push 생략)"), True)[1]

    # --- 마무리 ---
    banner("완료")
    files = sorted(glob.glob(os.path.join(OUT_DIR, "*.csv")))
    total = sum(os.path.getsize(f) for f in files) / 1024 / 1024
    print(f"   생성 파일 {len(files)}개 / {total:.1f}MB")
    for f in files:
        print(f"     {f}  ({os.path.getsize(f) / 1024:,.0f}KB)")
    print(f"\n   소요 {(datetime.datetime.now() - t0).seconds}초"
          + ("" if ok else "   ⚠ push 실패 — 위 메시지 확인"))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n중단했습니다.")
    except Exception:
        print("\n❌ 오류가 발생했습니다:\n")
        traceback.print_exc()
        sys.exit(1)