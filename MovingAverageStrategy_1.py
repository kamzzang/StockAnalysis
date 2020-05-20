import datetime, time

import talib as ta
import numpy as np
import pandas as pd
from pandas import DataFrame
import pandas.io.sql as pdsql

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib import dates
import matplotlib.font_manager as font_manager
import seaborn as sns

import mysql.connector

# 맑은고딕체
sns.set(style="whitegrid", font="Malgun Gothic", font_scale=1.5)
fp = font_manager.FontProperties(fname="C:\\WINDOWS\\Fonts\\malgun.TTF", size=15)

def comma_volume(x, pos=None):
    s = '{:0,d}K'.format(int(x / 1000))
    return s

def comma_price(x, pos=None):
    s = '{:0,d}'.format(int(x))
    return s

def comma_percent(x, pos=None):
    s = '{:+.2f}'.format(x)
    return s

major_date_formatter = dates.DateFormatter('%Y-%m-%d')
minor_date_formatter = dates.DateFormatter('%m')
price_formatter = ticker.FuncFormatter(comma_price)
volume_formatter = ticker.FuncFormatter(comma_volume)
percent_formatter = ticker.FuncFormatter(comma_percent)

MySQL_POOL_SIZE = 2

데이타베이스_설정값 = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'password',
    'database': 'database name',
    'raise_on_warnings': True,
}

class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)

    def _timestamp_to_mysql(self, value):
        return value.to_datetime()


def mysqlconn():
    conn = mysql.connector.connect(pool_name="stockpool", pool_size=MySQL_POOL_SIZE, **데이타베이스_설정값)
    conn.set_converter_class(NumpyMySQLConverter)
    return conn


# 데이타를 기간에 맞게 잘라냄
def 기간(dataframe, 시작기간=None, 종료기간=None):
    df = dataframe.copy()

    if (시작기간 is None) and (종료기간 is None):
        pass
    elif (시작기간 is None) and not (종료기간 is None):
        df = df[:종료기간]
    elif not (시작기간 is None) and (종료기간 is None):
        df = df[시작기간:]
    elif not (시작기간 is None) and not (종료기간 is None):
        df = df[시작기간:종료기간]

    return df


# 종목코드의 정보를 읽음
def get_info(code):
    query = """
        select 시장구분, 종목코드, 종목명, 주식수, 전일종가*주식수 as 시가총액
        from 종목코드
        where 종목코드 = '%s'
    """ % code
    conn = mysqlconn()
    df = pdsql.read_sql_query(query, con=conn)
    conn.close()

    for idx, row in df.iterrows():
        시장구분, 종목코드, 종목명, 주식수, 시가총액 = row

    return (시장구분, 종목코드, 종목명, 주식수, 시가총액)


# 지정한 종목의 가격/거래량 정보를 읽어 가공
def get_price(code, 시작일자=None, 종료일자=None):
    if 시작일자 == None and 종료일자 == None:
        query = """
        SELECT 일자, 시가, 고가, 저가, 종가, 거래량
        FROM 일별주가
        WHERE 종목코드='%s'
        ORDER BY 일자 ASC
        """ % (code)
    if 시작일자 != None and 종료일자 == None:
        query = """
        SELECT 일자, 시가, 고가, 저가, 종가, 거래량
        FROM 일별주가
        WHERE 종목코드='%s' AND 일자 >= '%s'
        ORDER BY 일자 ASC
        """ % (code, 시작일자)
    if 시작일자 == None and 종료일자 != None:
        query = """
        SELECT 일자, 시가, 고가, 저가, 종가, 거래량
        FROM 일별주가
        WHERE 종목코드='%s' AND 일자 <= '%s'
        ORDER BY 일자 ASC
        """ % (code, 종료일자)
    if 시작일자 != None and 종료일자 != None:
        query = """
        SELECT 일자, 시가, 고가, 저가, 종가, 거래량
        FROM 일별주가
        WHERE 종목코드='%s' AND 일자 BETWEEN '%s' AND '%s'
        ORDER BY 일자 ASC
        """ % (code, 시작일자, 종료일자)

    conn = mysqlconn()
    df = pdsql.read_sql_query(query, con=conn)
    conn.close()

    df.fillna(0, inplace=True)
    df.set_index('일자', inplace=True)


    # 추가 컬럼이 필요한 경우에 이 곳에 넣으시오
    df['MA20'] = df['종가'].rolling(window=20).mean()
    # 가중이동평균을 이용하는 경우
    # df['MA20'] = ta.WMA(np.array(df['종가'].astype(float)), timeperiod=20)
    df['전일MA20'] = df['MA20'].shift(1)

    df['MA240'] = df['종가'].rolling(window=240).mean()
    df['전일MA240'] = df['MA240'].shift(1)

    df.dropna(inplace=True)

    return df


# 이동평균을 이용한 백테스트 로봇
class CRobotMA(object):
    def __init__(self, 종목코드='122630'):
        self.info = get_info(code=종목코드)
        self.df = get_price(code=종목코드, 시작일자=None, 종료일자=None)

    # 투자 실행
    def run(self, 투자시작일=None, 투자종료일=None, 투자금=1000 * 10000):
        self.투자금 = 투자금
        self.portfolio = []  # [일자, 매수가, 수량]

        df = 기간(self.df, 시작기간=투자시작일, 종료기간=투자종료일)

        계좌평가결과 = []
        거래결과 = []
        #         for idate, row in df[['시가','종가','MA20','전일MA20','MA240','전일MA240']].iterrows():
        #             시가, 종가, MA20, 전일MA20, MA240, 전일MA240 = row
        for idate, row in df[['시가', '종가', 'MA20', '전일MA20']].iterrows():

            시가, 종가, MA20, 전일MA20 = row

            # 매수 매도 부분만 수정하면 다른 알고리즘 적용 가능
            # 매수
            ##############################################################
            매수조건 = 시가 > 전일MA20  # and 전일MA20 > 전일MA240
            if 매수조건 == True and len(self.portfolio) == 0:
                수량 = self.투자금 // 시가
                매수가 = 시가
                self.투자금 = self.투자금 - int((매수가 * 수량) * (1 + 0.00015))
                self.portfolio = [idate, 매수가, 수량]

            # 매도
            ##############################################################
            매도조건 = 시가 < 전일MA20
            if 매도조건 == True and len(self.portfolio) > 0:
                매도가 = 시가
                [매수일, 매수가, 수량] = self.portfolio
                수익 = (매도가 - 매수가) * 수량
                self.투자금 = self.투자금 + int((매도가 * 수량) * (1 - 0.00315))
                self.portfolio = []

                거래결과.append([idate, 매수가, 매도가, 수량, 수익, self.투자금])

            # 매일 계좌 평가하여 기록
            ##############################################################
            if len(self.portfolio) > 0:
                [매수일, 매수가, 수량] = self.portfolio
                매수금액 = 매수가 * 수량
                평가금액 = 종가 * 수량
                총자산 = self.투자금 + 평가금액
            else:
                매수가 = 0
                수량 = 0
                매수금액 = 0
                평가금액 = 0
                총자산 = self.투자금

            계좌평가결과.append([idate, 종가, self.투자금, 매수가, 수량, 매수금액, 평가금액, 총자산])

        # 거래의 최종 결과
        if (len(df) > 0):
            거래결과.append([df.index[-1], 0, 0, 0, 0, self.투자금])
            self.거래결과 = DataFrame(data=거래결과, columns=['일자', '매수가', '매도가', '수량', '수익', '투자금'])
            self.거래결과.set_index('일자', inplace=True)

            self.계좌평가결과 = DataFrame(data=계좌평가결과, columns=['일자', '현재가', '투자금', '매수가', '수량', '매수금액', '평가금액', '총자산'])
            self.계좌평가결과.set_index('일자', inplace=True)
            self.계좌평가결과['MA20'] = self.계좌평가결과['현재가'].rolling(window=60).mean()
            self.계좌평가결과['총자산MA60'] = self.계좌평가결과['총자산'].rolling(window=60).mean()

            return True
        else:
            return False

    def report(self, out=True):
        _총손익 = self.거래결과['수익'].sum()
        if out == True:
            print('총손익(Total Net Profit) %s' % comma_price(x=_총손익))

        _이익거래횟수 = len(self.거래결과.query("수익>0"))
        _총거래횟수 = len(self.거래결과)
        _승률 = _이익거래횟수 / _총거래횟수
        if out == True:
            print('승률(Percent Profit) %s/%s = %s' % (_이익거래횟수, _총거래횟수, comma_percent(x=_승률)))

        _평균이익금액 = self.거래결과.query("수익>0")['수익'].mean()
        _평균손실금액 = self.거래결과.query("수익<0")['수익'].mean()
        if out == True:
            print("평균이익금액(Ratio Avg Win) %s" % comma_price(x=_평균이익금액))
            print("평균손실금액(Ratio Avg Loss) %s" % comma_price(x=_평균손실금액))

        _최대수익금액 = self.거래결과['수익'].max()
        _최대손실금액 = self.거래결과['수익'].min()
        if out == True:
            print("1회거래 최대수익금액 %s" % comma_price(x=_최대수익금액))
            print("1회거래 최대손실금액 %s" % comma_price(x=_최대손실금액))

        _days = 60
        _MDD = np.max(self.계좌평가결과['총자산'].rolling(window=_days).max() - self.계좌평가결과['총자산'].rolling(window=_days).min())
        if out == True:
            print('%s일 최대연속손실폭(Maximum DrawDown) %s' % (_days, comma_price(x=_MDD)))

        return (_이익거래횟수, _총거래횟수, _총손익)

    def graph(self):
        df = self.계좌평가결과
        dfx = self.거래결과

        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 15), sharex=True)
        fig.suptitle("%s (%s)" % (self.info[2], self.info[1]), fontsize=15)  # (시장구분, 종목코드, 종목명, 주식수, 시가총액)

        ax = df[['현재가', 'MA20']].plot(ax=ax1)
        ax.xaxis.set_major_formatter(major_date_formatter)
        ax.yaxis.set_major_formatter(price_formatter)
        ax.set_ylabel('가격', fontproperties=fp)
        ax.set_xlabel('', fontproperties=fp)
        ax.legend(loc='best')

        ax = df[['총자산', '총자산MA60']].plot(ax=ax2)
        ax.xaxis.set_major_formatter(major_date_formatter)
        ax.yaxis.set_major_formatter(price_formatter)
        ax.set_ylabel('계좌평가결과', fontproperties=fp)
        ax.set_xlabel('', fontproperties=fp)
        ax.legend(loc='best')

        ax = dfx[['수익']].plot(ax=ax3, style='-o')
        ax.xaxis.set_major_formatter(major_date_formatter)
        ax.yaxis.set_major_formatter(price_formatter)
        ax.set_ylabel('거래결과', fontproperties=fp)
        ax.set_xlabel('', fontproperties=fp)
        ax.legend(loc='best')


robot = CRobotMA(종목코드='000020')
robot.run(투자시작일='2000-01-01', 투자종료일='2020-05-01', 투자금=1000 * 10000)
print(robot.report())

robot.graph()

print(robot.계좌평가결과.tail(10))

print(robot.거래결과.tail())


