
import pandas as pd
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
from glob import glob

font_dirs = ['./font'] # 폰트경로
font_files = fm.findSystemFonts(fontpaths=font_dirs)
for font_file in font_files:
    fm.fontManager.addfont(font_file)
plt.rcParams['font.family'] = 'Noto Sans CJK KR'


sido_list = ['서울', '부산', '대구', '인천', '광주', '대전', '울산', '세종', '경기',
               '강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주']

# 입학학생수 / 입학정원
def calculate_rate(d): 
    full, number =d['입학정원'],d['입학자수']
    if full==0:
        return -1
    return number/full

# 시도 행정구 이름으로 인덱스 생성 함수
def create_new_index(d):
    sido, sigungu = d['시도'],d['행정구']
    return str(sido)+'|' + str(sigungu)

# 데이터 중 행정구가 "서울 강남구" 처럼 시도가 같이 나오는 경우 분리
def split_hjgu(d):
    d = d['행정구역']
    if type(d) == float:
        return None
    return d.split()[-1]

def split_hjgu_new(d):
    d = d['행정구']
    if type(d) == float:
        return None
    return d.split()[-1]

# 하이픈인 데이터 0 으로 치환
def fill_hyphen(d):
    if d == '-':
        return 0
    return d

# 시도 + 행정구 만 있는 데이터만 거르기 위한 필터 제작 함수
def filter_total_data(d):
    d=d['index']
    first, second = d.split('|')
    if first =="세종" and second=="계":
        return True
    elif second=="계":
        return False
    else:
        return True

def filter_total_data_with_nan(d):
    d=d['index']
    first, second = d.split('|')
    if first =="세종" and second=="계":
        return True
    elif second=="계":
        return None
    elif second=='nan':
        return None
    else:
        return True
if __name__ == '__main__':
    full_files = glob('./data/*_full.xlsx')
    num_files = glob('./data/*_number.xlsx')
    full_files = sorted(full_files)
    num_files = sorted(num_files)


    result = []
    ## 모든 5년간 데이터 merge
    for i in range(len(num_files)):
        year = num_files[i].split('/')[-1].split('_')[0]
        df_number = pd.read_excel(num_files[i])
        df_full = pd.read_excel(full_files[i])

        if '행정구' not in df_number.columns:
            df_number['행정구'] = df_number.apply(split_hjgu, axis=1)
        if '행정구'  not in df_full.columns:
            df_full['행정구'] = df_full.apply(split_hjgu, axis=1)

        df_number['행정구역'] = df_number['행정구']
        df_full['행정구역'] = df_full['행정구']
        df_number = df_number.applymap(fill_hyphen)
        df_full = df_full.applymap(fill_hyphen)
        if '입학정원' not in df_full.columns:
            df_full['입학정원'] = df_full['전체']
            df_number['입학자수'] = df_number['전체']
        df_list = list()
        df_full['rename_index']= df_full.apply(create_new_index,axis=1)
        df_number['rename_index']= df_number.apply(create_new_index,axis=1)
        
        df1=df_full[['rename_index','시도','행정구역','입학정원']]
        df2=df_number[['rename_index','시도','행정구역','입학자수']]
        for isido in sido_list:
            tmp_df=pd.merge(df1[(df1['시도']==isido)                            
                                &(df1['행정구역']!='총계')\
                                &(df1['행정구역']!='행정구역')][['시도','행정구역','입학정원']], 
                            df2[(df2['시도']==isido)\
                                
                                &(df2['행정구역']!='총계')\
                                &(df2['행정구역']!='행정구역')][['시도','행정구역','입학자수']], 
                            how='outer', on=None)
            
            tmp_df.columns=['시도','행정구','입학정원','입학자수']
            tmp_df['index'] = tmp_df.apply(create_new_index, axis=1)
            
            tmp_df = tmp_df.set_index('index')
            df_list.append(tmp_df)
        
        df=pd.DataFrame()
        for idf in df_list:
            df = df.append(idf)
        df[year+'|rate']=df.apply(calculate_rate,axis=1)
        df.columns=['시도','행정구역',year+'|입학정원',year+'|입학자수',year+'|rate']
        df = df[['시도',year+'|입학정원',year+'|입학자수',year+'|rate']]
        result.append(df)


    # outer join
    df1=pd.merge(left = result[0] , right = result[1], how = "outer", on = 'index')
    df2=pd.merge(left = result[2] , right = result[3], how = "outer", on = 'index')
    df3 = pd.merge(left = df1, right=df2, how="outer", on="index")
    final_df = pd.merge(left = df3, right=result[4], how="outer", on="index") # 최종 데이터프레임


    final_df =final_df.reset_index()
    final_df['filter'] = final_df.apply(filter_total_data_with_nan,axis=1)
    df = final_df[final_df['filter']==True] # 시도|행정구 로 생성된 데이터행만 뽑음



    df=df[['index','시도_x_x',
        '2016|입학정원','2016|입학자수','2016|rate',
        '2017|입학정원','2017|입학자수','2017|rate',
        '2018|입학정원','2018|입학자수','2018|rate',
        '2019|입학정원','2019|입학자수','2019|rate',
        '2020|입학정원','2020|입학자수','2020|rate', 'filter'
    ]]
    df = df.set_index('index')

    df.columns=['시도']+df.columns[1:].tolist()
    df[df.columns[:-1]].head()


    """
    df.iloc[63:69] 

    세종시의 경우 세종|전체, 세종|세종 으로 되어 있는 경우가 있음 이를 제거
    """


    df.loc['세종|세종','2018|입학정원':'2019|rate'] = [4603,5119,1.112101,4603,5177,1.124701] # 데이터 채우기



    df = df.drop(['세종|계']) # 필요없는 데이터 제거


    df = df[df.columns[0:-1]].reset_index()

    print(df)
    df.to_json('./data/data.json', orient='records',force_ascii=False) # 최종 데이터 파일 json 으로 저장




