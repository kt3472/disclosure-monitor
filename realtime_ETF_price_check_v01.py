#!__*__coding:utf-8__*__

import time, datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from slacker import Slacker
import websocket
import os

# chromedriver.exe Down link : https://chromedriver.chromium.org/downloads
# 셀레니움라이브러리와 크롬드라이브 프로그램을 사용
driver = webdriver.Chrome('./chromedriver.exe')

# 작업 디렉토리 설정
# os.chdir("C:/work/spread-monitor")

# KBSTAR ETF 리스트
etf_list = pd.read_excel("./ETF-list/KBSTARETF_list_1101.xlsx")


# 가격비교를 위한 데이터프레임
price_check_dataform = pd.DataFrame({"code":[],"name":[], "price_1":[], "price_2":[],"diff_12":[] },
                      columns = ["code","name", "price_1", "price_2", "diff_12"])


# slack 메세지 발송함수

def notification(message):
    
    token = 'Slack에서 생성한 token을 입력'
    slack = Slacker(token)
    slack.chat.post_message('etf', message)


# 실시간 가격, 전일종가 추출 함수

def price_check(str_data):
    b = str_data.find("\n")
    c = str_data[b+1:].find("\n")
    
    d = str_data.find("전일")
    e = str_data.find("고가")
        
    return str(str_data[b+1:b+c+1]).replace(",",""), str(str_data[d+3:e-1]).replace(",","") 


# 최초 실행 or 재실행 여부를 판단하기위한 스위치변수 switch == 0 이면 최초실행
switch = 0

# 가격비교를 위한 데이터프레임의 인덱스 변수
index_count = 0

# 장종료시점(15:30)까지 계속 모니터링
while True :

    for i in range(len(etf_list)):
        
        # KBSTAR ETF 코드추출하고 크롬드라이브를 통해 해당 ETF종목의 페이지를 오픈
        etf_code = etf_list['code2'][i]
        code = "A"+ str(etf_code)

        url = "https://finance.daum.net/quotes/{}#home".format(code)
        driver.get(url)
        print(i, etf_code, url)
        select_1 = driver.find_elements_by_tag_name("span") # 실시간 가격의 html tag name은 <span>

        # 정상적으로 크롤링이 되었다면 tag name <span>은 34보다 커야됨, 정상적으로 수신이 될때까지 재수신
        while len(select_1) < 35 :
            time.sleep(1)
            print("재수신_type1")
            driver.get(url)
            print(i, etf_code, url)
            select_1 = driver.find_elements_by_tag_name("span")        

        
        # 크롤링한 tag name <span>들중 34번째에 실시간 정보가 있음
        a = str(select_1[34].text)
        
        
        # 크롤링한 tag name <span>들중 34번째에 카카오라는 단어가 들어 있으면 비정상 수신, 정상적으로 수신이 될때까지 재수신
        while a == "" or a.find("카카오") > -1:

            driver.get(url)
            select_1 = driver.find_elements_by_tag_name("span")
            a = str(select_1[34].text)
            time.sleep(1)
            print("재수신_type2")


        #if a is None or a.find("카카오") > -1:
        #   print("데이터가 없음")

        
        # 실시간가격, 전일종가를 추출하기 위한 price_check 함수 실행
        price_result = price_check(a)

        code = str(etf_list['code2'][i])
        name = str(etf_list['name'][i]) 

        
        # 최초 실행이라면 price_1은 전일종가, price_2는 현재가격
        if switch == 0:
            price_1 = int(price_result[0])
            price_2 = int(price_result[1])
            diff_12 = (price_1 / price_2) -1
          
        
        # 최초 실행이 아니라면 price_1은 이전 현재가격, price_2는 현재가격
        else :
            
            price_1 = price_check_dataform['price_2'][i]
            price_2 = int(price_result[0])
            
            
            diff_12 = (price_2 / price_1) -1
            
        
        # 가격비교를 위한 데이터프레임에 입력

        price_check_dataform.ix[index_count] = [code, name, price_1, price_2, diff_12]
        index_count = index_count + 1
        
        # 이전 가격대비 +2.00%, -2.00% 이상 변동 발생시, slack메신저로 메세지 발송

        if diff_12 > 0.03 or diff_12 < -0.03 :
            
            warr = "[이상급등락] " + str(code) +"  "+ str(name) +"  " + "등락률 : " + str(round(diff_12*100,2)) + "%"
            print(warr)
            notification(warr)           

        #print(price_result)
        print(price_1, price_2)
        
        print(round(diff_12*100,3))

        time.sleep(1)    
     
    index_count = 0
    
    # 최초 실행 후 switch변수값을 1로 변경하여 재실행임을 표시 
    switch = 1
    
    # 15:30 이후 실행 종료
    now = datetime.datetime.now()
    end_time = now.replace(hour = 15, minute = 35, second = 0, microsecond = 0)
    
    if now > end_time:
        
        print("이상급등락 모니터링을 종료합니다.")
        print(now)
        driver.quit()
        break