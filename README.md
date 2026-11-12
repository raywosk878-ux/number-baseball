# 숫자야구 (모바일 멀티플레이)

## 로컬 실행
pip install -r requirements.txt
python app.py   # http://localhost:5000

## Render 배포
1. 이 폴더를 GitHub 저장소로 올리기
2. Render → New → Web Service → 저장소 선택 (render.yaml 자동 인식)
   - Build: pip install -r requirements.txt
   - Start: gunicorn -w 1 --threads 100 -b 0.0.0.0:$PORT app:app
3. 배포된 주소를 휴대폰으로 공유 (방 만든 뒤 '공유' 버튼 → ?room=코드 링크)

※ 워커는 반드시 1개(-w 1). 방 정보가 메모리에 있어서 워커가 여러 개면 방이 갈라짐.
※ 무료 플랜은 15분 무접속 시 잠들어서 첫 접속이 30초~1분 걸림 → 시작 전에 한 번 미리 열어두기.
