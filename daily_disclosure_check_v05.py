#!__*__coding:utf-8__*__

import feedparser
import pandas as pd
import time, datetime
import numpy as np
from slacker import Slacker
import websocket
import os

# 작업디렉토리 설정
# os.chdir("D:/Users/user/Desktop/py/gongci")

# slack 메세지 발송함수 
# notification_kospi : kospi채널에 발송
# notification_kosaq : kosdaq채널에 발송
# notification_newlistings : newlistings채널에 발송
# notification_kosaq : div채널에 발송

def notification_kospi(message):
    
    token = 'Slack에서 생성한 token을 입력'
    slack = Slacker(token)
    slack.chat.post_message('kospi', message)
    
def notification_kosdaq(message):
    token = 'Slack에서 생성한 token을 입력'
    slack = Slacker(token)
    slack.chat.post_message('kosdaq', message)
    
def notification_newlistings(message):
    token = 'Slack에서 생성한 token을 입력'
    slack = Slacker(token)
    slack.chat.post_message('new_listings', message)
    
def notification_div(message):
    token = 'Slack에서 생성한 token을 입력'
    slack = Slacker(token)
    slack.chat.post_message('div_2019', message)


# ETF, ELW, 선박투자회사 등 상장기업이 아닌 공시 및 영향력이 없는 공시를 제외하기 위함
# 공시 분류를 위한 단어장 : 해당 단어가 공시 title에 들어가 있으면 slack 메신저를 발송하지 않음


out_word = ["상장지수증권", "주식워런트증권", "ELW", "투자설명서", "파생결합증권", "수익증권", "기업설명회",
            "워런트", "코스닥", "유가증권시장", "공매도", "대량", "임원", "기타파생결합사채", "증권발행실적보고서",
            "파생결합사채","주주총회소집결의", "투자설명서","특수관계인","소수계좌","담보", "단일판매", "사채",
            "KODEX", "TIGER", "ARIRANG", "KINDEX", "KBSTAR", "KOSEF", "KTOP", "HANARO", "TREX", "SMART", "FOCUS",
            "마이티", "파워", "마이다스", "증권발행실적보고서","일괄신고추가서류","사외이사의선임", "대표이사변경",
            "주주총회소집공고","동일인등출자계열회사와의상품", "지정자문인","주식명의개서정지","본점소재지변경",
            "최대주주등소유주식변동신고서", "약관에의한금융거래시계열금융회사의거래상대방의공시", "임시주주총회결과",
            "상호변경안내","타인에대한채무보증결정","단일계좌 거래량 상위종목","스팩","신규시설투자","임시주주총회 결과",
            "전환가액의조정", "선박투자회사", "참고서류","전환가액의 조정","주식매수선택권부여에관한신고","대표이사 변경 예정사항",
            "집중일", "분기보고서", "사업보고서","영업(잠정)실적","상황보고서", "채무인수결정","소액공모공시서류",
            "증권신고서(채무증권)", "증권신고서(지분증권)","금전대여결정","유형자산 양수 결정","주주총회소집 결의","매매관여 과다종목",
            "단기차입금증가결정", "타인에 대한 채무보증 결정","소액공모실적보고서","계열금융회사의약관에의한금융거래"]

today_date = str(datetime.datetime.today().strftime("%Y%m%d"))

in_word = "신규상장"
in_word_div = "현물배당 결정"
in_word_div1 = "현물 배당 결정"

# 공시데이터 기록을 위한 데이터 프레임 생성

gongci_data = pd.DataFrame({"check":[],"title":[],"time_pub":[], "link":[]}, columns = ["check","title","time_pub","link"])


# 공시데이터 기록 데이터프레임의 인덱스 변수

index_count = 0  

# 제외단어 카운트 변수

out_word_check_count = 0  

# 한국거래소 공시 데이터 feeding url ("searchCorpName=&currentPageSize=1000" -> 최근 1000개 까지 조회)

rss_url = "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=1000"

# 최초 실시간 데이터 피딩

start_gongci_feed = feedparser.parse(rss_url)

while True:
    
    # 현재 실시간 데이터 피딩
                
    current_gongci_feed = feedparser.parse(rss_url)
    
    print(len(current_gongci_feed.entries), len(start_gongci_feed.entries))
    
    # 가장 최근 피딩 데이터와 이전 피딩 데이터 비교
    if len(current_gongci_feed.entries) == len(start_gongci_feed.entries):
                
        pass

    else:
        
        # 이전 피딩 데이터에 없는 최근 피딩데이터를 읽어옴
        # (최근 피딩 데이터 인덱스 - 이전 피딩데이터 인덱스)로 최근 피딩 데이터만 읽어옴
        # 인덱스는 가장 최근 0 부터 시작
        
        for i in range(len(current_gongci_feed.entries)-len(start_gongci_feed.entries)):
            
            # 기록데이터 프레임의 인덱스를 가산함

            index_count = index_count + 1

            company_name = current_gongci_feed.entries[i]['author']
            
             # 유가증권 / 코스닥시장을 구분

            if company_name[1] == "유":

                # current_title : 공시제목
                # gongci_link : 공시링크
                # published_time : 공시시각
                
                current_title = current_gongci_feed.entries[i]['title']
                gongci_link = current_gongci_feed.entries[i]['link']
                published_time = current_gongci_feed.entries[i]['published']
                
                # 공시제목에 제외단어를 포함하고 있는지 비교

                for j in out_word:

                    if str(current_title).find(j) > 0:

                        out_word_check_count = out_word_check_count + 1

                    else:
                        pass
                
                # PC에 출력

                #print(current_title, published_time)
                #print(gongci_link)
                #print(out_word_check_count)

                title_pub = current_title + "  " + published_time

                # 제외단어를 포함하고 있지 않은 공시만 slack 메신저로 발송
                if out_word_check_count == 0 :

                    print(current_title, published_time)

                    print("발송")

                    notification_kospi(title_pub)
                    notification_kospi(gongci_link)
                    #notification_kospi(out_word_check_count)

                else: 
                    pass
                
                # 신규상장은 종목은 신규상장 체널로도 발송
                if str(current_title).find(in_word) >= 0 :

                    print(current_title, published_time)

                    print("신규상장 발송")

                    notification_newlistings(title_pub)
                    notification_newlistings(gongci_link)
                    
                else: 
                    pass
                
                # 배당관련 공시는 배당공시 체널로도 발송
                if str(current_title).find(in_word_div) >= 0 or str(current_title).find(in_word_div1) >= 0:

                    print(current_title, published_time)

                    print("배당관련 공시 발송")

                    notification_div(title_pub)
                    notification_div(gongci_link)
                    
                else: 
                    pass
                

            else:

                current_title = current_gongci_feed.entries[i]['title']
                gongci_link = current_gongci_feed.entries[i]['link']
                published_time = current_gongci_feed.entries[i]['published']

                for j in out_word:

                    if str(current_title).find(j) > 0:

                        out_word_check_count = out_word_check_count + 1

                    else:
                        pass            

                #print(current_title, published_time)
                #print(gongci_link)
                #print(out_word_check_count)

                title_pub = current_title + "  " + published_time

                if out_word_check_count == 0 :

                    print(current_title, published_time)

                    print("발송")

                    notification_kosdaq(title_pub)
                    notification_kosdaq(gongci_link)
                    #notification_kosdaq(out_word_check_count)

                else: 
                    pass
                
                
                # 신규상장은 종목은 신규상장 체널로도 발송
                if str(current_title).find(in_word) >= 0 :

                    print(current_title, published_time)

                    print("신규상장 발송")

                    notification_newlistings(title_pub)
                    notification_newlistings(gongci_link)
                    
                else: 
                    pass
                
                # 배당관련 공시는 배당공시 체널로도 발송
                if str(current_title).find(in_word_div) >= 0 or str(current_title).find(in_word_div1) >= 0:

                    print(current_title, published_time)

                    print("배당관련 공시 발송")

                    notification_div(title_pub)
                    notification_div(gongci_link)
                    
                else: 
                    pass
            
            # 공시 데이터를 프레임에 순차적으로 입력                        

            check = str(out_word_check_count)
            title = str(current_title)
            time_pub = str(published_time)
            link = str(gongci_link)
            
            #print(time_no, code, name, ask, bid, diff_1, SP, check_1)
            gongci_data.ix[index_count] = [check, title, time_pub, link]
            
            # 다음 회 공시 체크를 위해 제외단어 체크 변수를 초기화
            out_word_check_count = 0
        
        # 최근 피딩데이터는 이제 이전 피딩 데이터로 바뀜
        start_gongci_feed = current_gongci_feed
    
    # 공시데이터 프레임을 엑셀파일로 저장    
    gongci_data.to_excel(today_date+".xlsx")
    
    # 1800초(30분) 단위로 다시 데이터 피딩    
    time.sleep(1800)
    
    
    # 오후 6시에 프로그램을 종료함    
    now = datetime.datetime.now()
    end_time = now.replace(hour = 18, minute = 0, second = 0, microsecond = 0)
    
    if now > end_time:
        
        print("기업공시 모니터링을 종료합니다.")
        print(now)
        break

        