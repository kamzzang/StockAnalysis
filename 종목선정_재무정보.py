import datetime, time

import talib as ta

import numpy as np
import pandas as pd
from pandas import DataFrame
import pandas.io.sql as pdsql


import mysql.connector


MySQL_POOL_SIZE = 2

데이타베이스_설정값 = {
    'host': '127.0.0.1',
    'user': 'user',
    'password': 'password',
    'database': 'database',
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

# 데이타를 기간에 맞게 잘라내는 함수
def 기간(dataframe, 시작기간=None, 종료기간=None):
    df = dataframe.copy()

    if (시작기간 is None) and (종료기간 is None):
        pass
    elif (시작기간 is None) and not(종료기간 is None):
        df = df[:종료기간]
    elif not(시작기간 is None) and (종료기간 is None):
        df = df[시작기간:]
    elif not(시작기간 is None) and not(종료기간 is None):
        df = df[시작기간:종료기간]

    return df

################################################################################
# 시가총액 순으로 종목 선정
def 시가총액(시장범위=['KOSPI','KOSDAQ'], 시가총액범위=[500,3000]):
    _시장범위 = ["'%s'" % i for i in 시장범위]

    query = """
        SELECT 시장구분, 종목코드, 종목명, 주식수, 감리구분, 상장일, 전일종가, 시가총액, 종목상태
        FROM 종목코드_주식
        WHERE (시장구분 IN (%s)) and (시가총액 between %s * (10000 * 10000) and %s * (10000 * 10000))
        ORDER BY 시가총액 DESC
    """ % (','.join(_시장범위), 시가총액범위[0], 시가총액범위[1])

    conn = mysqlconn()
    df = pdsql.read_sql_query(query, con=conn)
    conn.close()
    return df


################################################################################
# 그린브라트 방식으로 종목을 선정
def Greenblatt(날짜='2011-12-31', 기간구분='년간'):
    result = DataFrame()

    query = """
    SELECT A.날짜, A.기간구분, A.종목코드, C.종목명, B.종가, A.매출액, A.영업이익, A.당기순이익, A.자산총계, A.부채총계, A.자본총계, A.자본금, 
        A.부채비율, A.유보율, A.영업이익률, A.순이익률, A.ROA, A.ROE, A.EPS, A.BPS, A.DPS, A.PER, 1/A.PER as RPER, A.PBR, A.발행주식수, A.배당수익률, C.종목상태
    FROM 재무정보 A, (select 종목코드, 종가 from 일별주가 where 일자 = (select max(일자) from 일별주가 where 일자 <= '%s')) B, 종목코드 C
    WHERE 날짜='%s' and 기간구분='%s' and A.종목코드=B.종목코드 and A.종목코드=C.종목코드
    """ % (날짜, 날짜, 기간구분)

    conn = mysqlconn()
    df = pdsql.read_sql_query(query, con=conn)
    conn.close()

    df['rank1'] = df['ROA'].rank(ascending=False)
    df['rank2'] = df['RPER'].rank(ascending=False)
    df['ranksum'] = df['rank1'] + df['rank2']
    df['rank'] = df['ranksum'].rank(ascending=True)

    result = df.sort_values(['rank', 'rank1', 'rank2'], ascending=[True,True,True])

    return result


################################################################################
# 드레먼 방식으로 종목을 선정 - 역발상투자
def DavidDreman(날짜='2011-12-31', 기간구분='년간'):
    result = DataFrame()

    query = """
    SELECT A.날짜, A.기간구분, A.종목코드, C.종목명, B.종가, A.매출액, A.영업이익, A.당기순이익, A.자산총계, A.부채총계, A.자본총계, A.자본금, 
        A.부채비율, A.유보율, A.영업이익률, A.순이익률, A.ROA, A.ROE, A.EPS, A.BPS, A.DPS, A.PER, 1/A.PER as RPER, A.PBR, A.발행주식수, A.배당수익률, C.종목상태
    FROM 재무정보 A, (select 종목코드, 종가 from 일별주가 where 일자 = (select max(일자) from 일별주가 where 일자 <= '%s')) B, 종목코드 C
    WHERE 날짜='%s' and 기간구분='%s' and A.종목코드=B.종목코드 and A.종목코드=C.종목코드
    """ % (날짜, 날짜, 기간구분)

    conn = mysqlconn()
    df = pdsql.read_sql_query(query, con=conn)
    conn.close()

    df['rank1'] = df['ROA'].rank(ascending=False)
    df['rank2'] = df['PBR'].rank(ascending=True)
    df['rank3'] = df['PER'].rank(ascending=True)
    df['ranksum'] = df['rank1'] + df['rank2'] + df['rank3']
    df['rank'] = df['ranksum'].rank(ascending=True)

    result = df.sort_values(['rank', 'rank1', 'rank2', 'rank3'], ascending=[True,True,True,True])

    return result

################################################################################
# •보통 좋은종목을 선정하는 방법
def 좋은종목(날짜='2011-12-31', 기간구분='년간'):
    result = DataFrame()

    query = """
    SELECT A.날짜, A.기간구분, A.종목코드, C.종목명, B.종가, A.매출액, A.영업이익, A.당기순이익, A.자산총계, A.부채총계, A.자본총계, A.자본금, 
        A.부채비율, A.유보율, A.영업이익률, A.순이익률, A.ROA, A.ROE, A.EPS, A.BPS, A.DPS, A.PER, 1/A.PER as RPER, A.PBR, A.발행주식수, A.배당수익률, C.종목상태
    FROM 재무정보 A, (select 종목코드, 종가 from 일별주가 where 일자 = (select max(일자) from 일별주가 where 일자 <= '%s')) B, 종목코드 C
    WHERE 날짜='%s' and 기간구분='%s' and A.종목코드=B.종목코드 and A.종목코드=C.종목코드
    """ % (날짜, 날짜, 기간구분)

    conn = mysqlconn()
    df = pdsql.read_sql_query(query, con=conn)
    conn.close()

    df['rank1'] = df['영업이익률'].rank(ascending=False)
    df['rank2'] = df['ROE'].rank(ascending=False)
    df['rank3'] = df['PER'].rank(ascending=True)
    df['rank4'] = df['유보율'].rank(ascending=False)
    df['ranksum'] = df['rank1'] + df['rank2'] + df['rank3'] + df['rank4']
    df['rank'] = df['ranksum'].rank(ascending=True)

    result = df.sort_values(['rank','rank1','rank3','rank4'], ascending=[True,False,True,False])

    return result

################################################################################
# 영업이익 위주로 종목을 선정
def 영업이익(날짜='2011-12-31', 기간구분='년간', 정렬순서=True):
    result = DataFrame()

    query = """
    SELECT A.날짜, A.기간구분, A.종목코드, C.종목명, B.종가, A.매출액, A.영업이익, A.당기순이익, A.자산총계, A.부채총계, A.자본총계, A.자본금, 
        A.부채비율, A.유보율, A.영업이익률, A.순이익률, A.ROA, A.ROE, A.EPS, A.BPS, A.DPS, A.PER, 1/A.PER as RPER, A.PBR, A.발행주식수, A.배당수익률, C.종목상태
    FROM 재무정보 A, (select 종목코드, 종가 from 일별주가 where 일자 = (select max(일자) from 일별주가 where 일자 <= '%s')) B, 종목코드 C
    WHERE 날짜='%s' and 기간구분='%s' and A.종목코드=B.종목코드 and A.종목코드=C.종목코드
    """ % (날짜, 날짜, 기간구분)

    conn = mysqlconn()
    df = pdsql.read_sql_query(query, con=conn)
    conn.close()

    df['rank1'] = df['영업이익률'].rank(ascending=False)
    df['rank2'] = df['순이익률'].rank(ascending=False)
    df['rank3'] = df['유보율'].rank(ascending=False)
    df['ranksum'] = df['rank1'] + df['rank2'] + df['rank3']
    df['rank'] = df['ranksum'].rank(ascending=True)

    result = df.sort_values(['rank', 'rank1', 'rank2', 'rank3'], ascending=[정렬순서,False,False,False])

    return result
