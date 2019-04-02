# sise_crawler

네이버 금융 페이지를 파싱해서 일별 OHLCV 값들을 SQLite3 파일DB에 저장해줍니다.

(종목코드는 각자 알아서...)

initial.py : 데이터가 전혀 없는 상태에서 시세 정보 전체를 긁어오는 용도

sqlite_to_mysql.py: CHANGE 컬럼값 검증하고 mysql 로 시세 데이터 복사하기 (검증해보니 오차가 있어서 threshold값 이내면 허용하도록 하였다.)

batch_initial.py: shcodes.sample.py 를 복사하여 shcodes.py 로 만들고 원하는 코드를 추가후 다음과 같이 실행 (tee가 필요없으면 안써도 무방)
```
$ python3 -u batch_initial.py 2>&1 | tee batch.log
```

### TODO:

update.py: 실시간 시세 정보 업데이트하기
