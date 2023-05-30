import pandas as pd
import numpy as np



class DataCleaner:
    def clean_user_data(self, df):

        df['first_name'] = df['first_name'].astype('string')
        df['first_name'] = df['first_name'].str.replace('-', '').str.replace('.', '').str.replace(' ', '')

        df['first_name_check'] = df['first_name'].str.contains('[\d]', na=True)
        df = df[df['first_name_check']==False]
        df.drop(['first_name_check'], axis=1, inplace=True)

        df['last_name'] = df['last_name'].astype('string')
        df['last_name_check'] = df['last_name'].str.contains('[\d]', na=True)
        df = df[df['last_name_check']==False]
        df.drop(['last_name_check'], axis=1, inplace=True)

        df['email_address']= df['email_address'].astype('string')
        df['email_address_check'] = df['email_address'].str.contains('@', na=False)
        df = df[df['email_address_check']==True]
        df.drop(['email_address_check'], axis=1, inplace=True)

        df = df.dropna()

        df['country'] = df['country'].astype('category')
        df['country_code'] = df['country_code'].astype('category')
        mapping = {'GGB': 'GB'}
        df['country_code'] = df['country_code'].replace(mapping)

        df['phone_number']= df['phone_number'].str.replace('[^\\d\+\)\(]+', '')
        
        df['phone_number']= df['phone_number'].astype('string')
        df['phone_number_check'] = df['phone_number'].str.contains('[\d\+\)\(]+', na=False)
        df = df[df['phone_number_check']==True]
        df.drop(['phone_number_check'], axis=1, inplace=True)

        df['company'] = df['company'].astype('string')
        df['address'] = df['address'].astype('category')

        df['user_uuid']= df['user_uuid'].astype('string')
        df['uuid_to_check'] = df['user_uuid'].str.contains('[a-zA-Z\d-]*', na=False)
        df = df[df['uuid_to_check']==True]
        df = df.drop(['uuid_to_check'], axis=1)

        df.dropna()
        df.reset_index(drop=True, inplace=True)
        
        df['date_of_birth'] = df['date_of_birth'].str.replace('-', ' ').str.replace('/', ' ')
        df[['year', 'month', 'day']] = df['date_of_birth'].str.split(' ', expand = True)       

        index = 0
        while (index < 14806):
            search = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
            yr = df.loc[index, 'month']
            if str(df.loc[index, 'year']) in search:
               df.loc[index, 'month'] = df.loc[index, 'year']
               df.loc[index, 'year'] = yr
               index+=1
            else:
                index+=1

        df['month'] = df['month'].astype('category')
        mapping = {'Jan': '1', 'January': '1', 'Feb': '2', 'February': '2', 'March': '3', 'Apr': '4', 'April': '4', 'May': '5', 'June': '6', 'Jun': '6', 'July': '7', 'Jul': '7','Aug': '8', 'August': '8','Sep': '9', 'September': '9','Oct': '10', 'October': '10', 'Nov': '11', 'November': '11', 'Dec': '12', 'December': '12',}
        df['month'] = df['month'].replace(mapping)      

        df = df.drop('date_of_birth', axis='columns')
        
        df['date_of_birth']=df[['year', 'month', 'day']].apply(lambda x: '-'.join(x.values.astype(str)), axis='columns')
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format = '%Y-%m-%d')

        df = df.drop('month', axis='columns')
        df = df.drop('day', axis='columns')
        df = df.drop('year', axis='columns')     

        df.dropna()
        df.reset_index(drop=True, inplace=True)
        
        df['join_date'] = df['join_date'].str.replace('-', ' ').str.replace('/', ' ')
        df[['year', 'month', 'day']] = df['join_date'].str.split(' ', expand = True)       

        index = 0
        while (index < 14806):
            search = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
            yr = df.loc[index, 'month']
            if str(df.loc[index, 'year']) in search: 
               df.loc[index, 'month'] = df.loc[index, 'year']
               df.loc[index, 'year'] = yr
               index+=1
            else:
                index+=1

        df['month'] = df['month'].astype('category')
        mapping = {'Jan': '1', 'January': '1', 'Feb': '2', 'February': '2', 'March': '3', 'Apr': '4', 'April': '4', 'May': '5', 'June': '6', 'Jun': '6', 'July': '7', 'Jul': '7','Aug': '8', 'August': '8','Sep': '9', 'September': '9','Oct': '10', 'October': '10', 'Nov': '11', 'November': '11', 'Dec': '12', 'December': '12',}
        df['month'] = df['month'].replace(mapping)      

        df = df.drop('join_date', axis='columns')
        
        df['join_date']=df[['year', 'month', 'day']].apply(lambda x: '-'.join(x.values.astype(str)), axis='columns')
        df['join_date'] = pd.to_datetime(df['join_date'], format = '%Y-%m-%d')

        df = df.drop('month', axis='columns')
        df = df.drop('day', axis='columns')
        df = df.drop('year', axis='columns')       

        return df
    
    def clean_card_data(self, df):

        #cleaning card number, removing any values that are not numbers or the right length
        df = df.dropna()
        df['card_number'] = df['card_number'].astype('string')
        df['card_number'] = df['card_number'].str.replace('\D+', "")
        df.loc[df['card_number'].str.len() < 20]
        df.loc[df['card_number'].str.len() > 14]


        #cleaning expiry date, removing values with incorrect format
        df[['month', 'year']] = df['expiry_date'].str.split('/', expand = True)
        
        df = df.drop('expiry_date', axis='columns')

        df = df[pd.to_numeric(df['month'], errors='coerce').notnull()]
        df = df[pd.to_numeric(df['year'], errors='coerce').notnull()]
        df['month']= df['month'].astype('int64')
        df['year']= df['year'].astype('int64')

        df = df.dropna()

        df['month_check']= df['month'] < 13
        df = df[df['month_check']==True]
        df = df.drop(['month_check'], axis=1)
        df['year_check'] = df['year'] < 35
        df = df[df['year_check']==True]
        df = df.drop(['year_check'], axis=1)


        cols=['year', 'month']
        df['expiry_date']=df[cols].apply(lambda x: '/'.join(x.values.astype(str)), axis='columns')
        df['expiry_date'] = df['expiry_date'].astype('string')
        
        
        df = df.drop('month', axis='columns')
        df = df.drop('year', axis='columns')

        #cleaning card provider, convert to category and remove unique values
        df["card_provider"] = df["card_provider"].astype("category")
        
        inconsistent_categories = {'1M38DYQTZV', '5CJH7ABGDR', '5MFWFBZRM9', 'WJVMUO4QX6', 'XGZBYBYGUW', 'DE488ORDXY', 'OGJTXI6X1H', 'DLWF2HANZF', 'UA07L7EILH', 'BU9U947ZGV', 'NULL', 'NB71VBAHJE', 'JRPRLPIBZ2', 'TS8A81WFXV', 'JCQMU8FN85'}
        inconsistent_rows = df['card_provider'].isin(inconsistent_categories)
        df = df[~inconsistent_rows]
        
        #cleaning date payment confirmed, converting to datetime,removing incorrect values
        df.dropna()
        df.reset_index(drop=True, inplace=True)
        
        df['date_payment_confirmed']=df['date_payment_confirmed'].str.replace('-', ' ').str.replace('/', ' ')
        df[['year', 'month', 'day']] = df['date_payment_confirmed'].str.split(' ', expand = True)

        index4 = 0
        while index4 < 15284:
            yr = df.loc[index4, 'month']
            if str(df.loc[index4, 'year']).isalpha() == True:
                df.loc[index4, 'month'] = df.loc[index4, 'year']
                df.loc[index4, 'year'] = yr
                index4+=1
            else:
                index4+=1

        df['month'] = df['month'].astype('category')
        mapping = {'Jan': '1', 'January': '1', 'Feb': '2', 'February': '2', 'March': '3', 'Apr': '4', 'April': '4', 'May': '5', 'June': '6', 'Jun': '6', 'July': '7', 'Jul': '7','Aug': '8', 'August': '8','Sep': '9', 'September': '9','Oct': '10', 'October': '10', 'Nov': '11', 'November': '11', 'Dec': '12', 'December': '12',}
        df['month'] = df['month'].replace(mapping)
        
       

        df = df.drop('date_payment_confirmed', axis='columns')
        df['date_payment_confirmed']=df[['year', 'month', 'day']].apply(lambda x: '-'.join(x.values.astype(str)), axis='columns')
        df = df.drop('month', axis='columns')
        df = df.drop('day', axis='columns')
        df = df.drop('year', axis='columns')

        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='%Y/%m/%d', errors='coerce')
        

        #removing all null values
        df = df.dropna()

        return df

    def clean_store_data(self, df):

            ##PRE-SPECIFIC CLEANING

         df.loc[0, 'address']='World-Wide-Web\n'
         df.loc[0, 'longitude'] = 0.00
         df.loc[0, 'latitude'] = 0.00
         df.loc[0, 'locality'] = 'Great Britain'

         df = df.drop(columns=['lat'])

           ##CLEANING FLOAT/INTEGER
 
        ##CLEANING INDEX BY CONVERTING TO INTEGER, REMOVING INVALID VALUES
         df = df[pd.to_numeric(df['index'], errors='coerce').notnull()]
         df['index'] = df['index'].astype('int64')
        
        ##CLEANING LONGITUDE BY CONVERTING TO FLOAT, REMOVING INVALID VALUES
         
         df['longitude']= df['longitude'].astype('string')
         df['longitude_check'] = df['longitude'].str.contains('[.]', na=False)
         df = df[df['longitude_check']==True]
         df.drop(['longitude_check'], axis=1, inplace=True)
         df = df[pd.to_numeric(df['longitude'], errors='coerce').notnull()]
         df['longitude'] = df['longitude'].astype('float')
    
        ##CLEANING LATITUDE BY CONVERTING TO FLOAT, REMOVING INVALID VALUES
         df['latitude']= df['latitude'].astype('string')
         df['latitude_check'] = df['latitude'].str.contains('[.]', na=False)
         df = df[df['latitude_check']==True]
         df = df[pd.to_numeric(df['longitude'], errors='coerce').notnull()]
         df.drop(['latitude_check'], axis=1, inplace=True)
         
         df['latitude'] = df['latitude'].astype('float')


        ##CLEANING STAFF NUMBERS BY CONVERTING TO INTEGER, REMOVING INVALID VALUES
         df['staff_numbers']= df['staff_numbers'].astype('string')
         df['staff_numbers']= df['staff_numbers'].str.replace(r'[A-Za-z]', '')
         df['staff_numbers'] = df['staff_numbers'].astype('int64')
         

            ##CLEANING CATEGORIES

        ##CLEANING STORE_TYPE BY CONVERTING TO CATEGORIES, MOVING/REMOVING/MAPPING UNIQUE CATEGORIES
         df["store_type"] = df["store_type"].astype("category")
         
        ##CLEANING COUNTRY_CODE BY CONVERTING TO CATEGORIES, MOVING/REMOVING/MAPPING UNIQUE CATEGORIES
         df['country_code'] = df['country_code'].astype('category')
        
        ##CLEANING CONTINENT BY CONVERTING TO CATEGORIES, MOVING/REMOVING/MAPPING UNIQUE CATEGORIES
         df['continent'] = df['continent'].astype('category')
         mapping = {"eeAmerica": "America", "eeEurope": "Europe"}
         df["continent"] = df["continent"].replace(mapping)
         
            ##CLEANING STRINGS

        ##CLEANING STORE CODE BY CHECKING IF CORRECT LETTERS

         df= df.dropna()
         df.reset_index(drop=True, inplace=True)

         index = 0
         while index < 441:
          inval = df.loc[index, 'store_code'].count('-')
          if inval < 1:
               df.loc[index, 'store_code']=np.nan
               index+=1
          else:
               index+=1
          
        ##CLEANING ADDRESS BY INDEXING, REMOVING VALUES WITH INDEX < 1
         
         index2 = 0
         while index2 < 441:
             addlen = df.loc[index2, 'address'].count('\n')
             if addlen < 1:
                 df.loc[index2, 'address'] = np.nan
                 index2+=1
             else:
                 index2+=1

        ##CLEANING LOCALITY BY REMOVING NON ALBHABET VALUES
         index3 = 0
         while index3 < 441:
             local = str(df.loc[index3, 'locality']).replace(" ", "").replace("-", "").isalpha()
             if local == True:
                 index3+=1
             else:
                 df.loc[index3, 'locality'] = np.nan
                 index3+=1

           ##CLEANING DATETIME

        ##CLEANING OPENING DATE BY CONVERTING TO DATETIME, MAPPING AND MOVING VALUES THAT DON'T FIT FORMAT
      
         df['opening_date']=df['opening_date'].str.replace('-', ' ').str.replace('/', ' ')
         df[['year', 'month', 'day']] = df['opening_date'].str.split(' ', expand = True)

         index4 = 0
         while index4 < 436:
             yr = df.loc[index4, 'month']
             if str(df.loc[index4, 'year']).isalpha() == True:
                df.loc[index4, 'month'] = df.loc[index4, 'year']
                df.loc[index4, 'year'] = yr
                index4+=1
             else:
                 index4+=1

         df['month'] = df['month'].astype('category')
         mapping = {'Jan': '1', 'January': '1', 'Feb': '2', 'February': '2', 'March': '3', 'Apr': '4', 'April': '4', 'May': '5', 'June': '6', 'Jun': '6', 'July': '7', 'Jul': '7','Aug': '8', 'August': '8','Sep': '9', 'September': '9','Oct': '10', 'October': '10', 'Nov': '11', 'November': '11', 'Dec': '12', 'December': '12',}
         df['month'] = df['month'].replace(mapping)

         df = df.drop(columns=['opening_date'])
         df['opening_date']=df[['year', 'month', 'day']].apply(lambda x: '-'.join(x.values.astype(str)), axis='columns')
         df = df.drop('month', axis='columns')
         df = df.drop('day', axis='columns')
         df = df.drop('year', axis='columns')

         format = '%Y-%m-%d'
         df['opening_date'] = pd.to_datetime(df['opening_date'], format=format, errors='coerce')

         df['address'] = df['address'].astype('string')
         df['locality'] = df['locality'].astype('string')
         df['store_code'] = df['store_code'].astype('string')

        #  df.loc[0, 'address']='N/A'
        #  df.loc[0, 'longitude'] ='N/A'
        #  df.loc[0, 'latitude'] = 'N/A'
        #  df.loc[0, 'locality'] = 'N/A'

         return df
                
    def convert_products_weight(self, df):
        df = df.dropna()

        cells_to_divide = df['weight'].str.contains('kg',na=False)
        numeric_weights= df['weight'].str.extract(r'(\d+)').astype('float')
        numeric_weights.iloc[~cells_to_divide.values]=numeric_weights.iloc[~cells_to_divide.values].multiply(0.001)
        df['weight']=numeric_weights
        
        return df

    def clean_products_data(self, df):
        
        ##CLEAN INTEGERS/FLOATS
        
        ##CHANGE NAME OF INDEX
        df = df.rename(columns={'Unnamed: 0': 'Index'})
        
        ##CHANGE PRODUCT_PRICE BY REMOVING THE '£' THEN CONVERTING TO FLOAT
        df['product_price']=df['product_price'].astype('string')
        df['product_price']=df['product_price'].str.replace("£", "")
        df = df[pd.to_numeric(df['product_price'], errors='coerce').notnull()]
        df['product_price']=df['product_price'].astype('float64')

        ##CLEAN EAN BY CONVERTING TO INT64
        # df = df.loc[df['EAN'].str.len() < 15]
        df = df[pd.to_numeric(df['EAN'], errors='coerce').notnull()]
        df['EAN'] = df['EAN'].astype('int64')
        df = df.rename(columns={'EAN': 'ean'})

    #     ##CLEAN CATEGORY BY CONVERTING TO CATEGORY
        df['category'] = df['category'].astype('category')

    #     ##CLEAN REMOVED BY CONVERTING TO CATEGORY 
        df['removed'] = df['removed'].astype('category')

    #     ##CLEAN PRODUCT_NAME BY CONVERTING TO STRING
        df['product_name'] = df['product_name'].astype('string')

        ##CLEAN UUID BY CONVERTING TO STRING, REMOVING ALL NON-ALPHANURMERIC VALUES EXCEPT FOR '-'
        df['uuid']= df['uuid'].astype('string')
        df['uuid_to_check'] = df['uuid'].str.contains(r'^[a-zA-Z\d-]*$', na=False)
        df = df[df['uuid_to_check']==True]
        df = df.drop(['uuid_to_check'], axis=1)

        ##CLEAN PRODUCT CODE BY CONVERTING TO STRING, REMOVING ALL NON-ALPHANUMERIC VALUES EXCEPT FOR '-'
        df['product_code']= df['product_code'].astype('string')
        df['product_check'] = df['product_code'].str.contains(r'^[a-zA-Z\d-]*$', na=False)
        df = df[df['product_check']==True]
        df = df.drop(['product_check'], axis=1)
        
        ##CLEAN DATE_ADDED BY CONVERTING TO DATETIME, MAPPING AND REMOVING WRONG VALUES
        df = df.dropna()
        df.reset_index(drop=True, inplace=True)

        df['date_added']=df['date_added'].str.replace('-', ' ')
        df[['year', 'month', 'day']] = df['date_added'].str.split(' ', expand = True)

        index = 0
        while index < 1634:
             yr = df.loc[index, 'month']
             if str(df.loc[index, 'year']).isalpha() == True:
                df.loc[index, 'month'] = df.loc[index, 'year']
                df.loc[index, 'year'] = yr
                index+=1
             else:
                 index+=1

        mapping = {'October': 10, 'September': 9}
        df['month'] = df['month'].astype('category')
        df['month'] = df['month'].replace(mapping)

        df = df.drop(columns=['date_added'])
        df['date_added']=df[['year', 'month', 'day']].apply(lambda x: '-'.join(x.values.astype(str)), axis='columns')

        df = df.drop('month', axis='columns')
        df = df.drop('day', axis='columns')
        df = df.drop('year', axis='columns')

        format = '%Y-%m-%d'
        df['date_added'] = pd.to_datetime(df['date_added'], format=format, errors='coerce')

        return df
    
    def clean_orders_data(self,df):
        ##DROP FIRST_NAME, LAST NAME AND 1 COLUMNS AS TOO MANY NULL VALUES

        df.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)

        ##CLEAN ALL STRING DATATYPES BY CHECKING FOR ALPHANUMERIC AND '-' ONLY IN CODE.
        ##FOR SOME COLUMNS, A FORMAT CHECK WILL BE RUN BUT NOT NECESSARILY USED IF TOO 
        ##MANY NULL VALUES ARE RECIEVED BACK, AS THIS PRESENTS A LACK OF FORMAT CORRELATION

        ##store_code, this has no obvious format so just converting to string containing a-zA-Z0-9-

        df['store_code']= df['store_code'].astype('string')
        df['store_code_check'] = df['store_code'].str.contains(r'^[a-zA-Z\d-]*$', na=False)
        df = df[df['store_code_check']==True]
        df = df.drop(['store_code_check'], axis=1)

        ##date_uuid
        
        df['date_uuid']= df['date_uuid'].astype('string')
        df['date_uuid_check'] = df['date_uuid'].str.contains(r'^[a-zA-Z\d-]*$', na=False)
        df = df[df['date_uuid_check']==True]
        df = df.drop(['date_uuid_check'], axis=1)

        ##user_uuid
        
        df['user_uuid']= df['user_uuid'].astype('string')
        df['user_uuid_check'] = df['user_uuid'].str.contains(r'^[a-zA-Z\d-]*$', na=False)
        df = df[df['user_uuid_check']==True]
        df = df.drop(['user_uuid_check'], axis=1)

        ##product_code

        df['product_code']= df['product_code'].astype('string')
        df['product_code_check'] = df['product_code'].str.contains(r'^[a-zA-Z\d-]*$', na=False)
        df = df[df['product_code_check']==True]
        df = df.drop(['product_code_check'], axis=1)
        
        return df
    
    def clean_date_details(self, df):

        ##CLEANING TIMESTAMP BY SPLITTING TO SPECIFICS, CHECKING FOR NON-NUMERIC VALUES AND MAPPING/REMOVING
        ##BEFORE JOINING TOGETHER IN FORMAT HH:MM:SS AND CONVERTING TO DATETIME
        
        df['timestamp'] = df['timestamp'].replace(' ', ':')
        df[['hour', 'minute', 'second']] = df['timestamp'].str.split(':', expand = True)
        df = df.dropna()

        df['hour'] = df['hour'].astype('string')
        df['hour_check'] = df['hour'].str.isnumeric()
        df = df[df['hour_check']==True]
        df.drop(['hour_check'], axis=1, inplace=True)

        df['minute'] = df['minute'].astype('string')
        df['minute_check'] = df['minute'].str.isnumeric()
        df = df[df['minute_check']==True]
        df.drop(['minute_check'], axis=1, inplace=True)

        df['second'] = df['second'].astype('string')
        df['second_check'] = df['second'].str.isnumeric()
        df = df[df['second_check']==True]
        df.drop(['second_check'], axis=1, inplace=True)

        df['hour'] = df['hour'].astype('int')
        df['hour_size'] = df['hour']
        df= df[df['hour_size'] < 24]
        df.drop(['hour_size'], axis = 1, inplace=True)

        df['minute'] = df['minute'].astype('int')
        df['minute_size'] = df['minute']
        df= df[df['minute_size'] < 60]
        df.drop(['minute_size'], axis = 1, inplace=True)

        df['second'] = df['second'].astype('int')
        df['second_size'] = df['second']
        df= df[df['second_size'] < 60]
        df.drop(['second_size'], axis = 1, inplace=True)

        df.drop(['timestamp'], axis=1, inplace=True)
       

        df['timestamp']=df[['hour', 'minute', 'second']].apply(lambda x: '-'.join(x.values.astype(str)), axis='columns')

        df = df.drop('hour', axis='columns')
        df = df.drop('minute', axis='columns')
        df = df.drop('second', axis='columns')

        format = '%H-%M-%S'
        df['timestamp'] = pd.to_datetime(df['timestamp'], format=format, errors='coerce').dt.time


        # df = df.drop('hour', axis='columns')
        # df = df.drop('minute', axis='columns')
        # df = df.drop('second', axis='columns')

        #df['timestamp'] = pd.datetime(df['timestamp'])

        ##CLEANING YEAR/MONTH/DAY THEN JOINING INTO A DATE COLUMN FOR EASE OF READING. CHECKING ALL DATA IS NUMERIC AND
        ##CORRECT FORMAT. CAN BE EDITED TO CREATE LESS NULL VALUES BY FORMATTING

        df['year'] = df['year'].astype('string')
        df['year_check'] = df['year'].str.isnumeric()
        df = df[df['year_check']==True]
        df.drop(['year_check'], axis=1, inplace=True)

        df['month'] = df['month'].astype('string')
        df['month_check'] = df['month'].str.isnumeric()
        df = df[df['month_check']==True]
        df.drop(['month_check'], axis=1, inplace=True)

        df['day'] = df['day'].astype('string')
        df['day_check'] = df['day'].str.isnumeric()
        df = df[df['day_check']==True]
        df.drop(['day_check'], axis=1, inplace=True)

        df['month'] = df['month'].astype('int')
        df['month_size'] = df['month']
        df= df[df['month_size'] < 13]
        df.drop(['month_size'], axis = 1, inplace=True)

        df['day'] = df['day'].astype('int')
        df['day_size'] = df['day']
        df= df[df['day_size'] < 32]
        df.drop(['day_size'], axis = 1, inplace=True)

        ##CLEANING TIME_PERIOD BY CONVERTING TO CATEGORY, ANY INCORRECT CATEGORIES WILL BE MAPPED/REMOVED

        df['time_period'] = df['time_period'].astype('category')

        ##CLEANING DATE_UUID BY CONVERTING TO STRING, ENSURING ONLY ALPHANUMERIC VALUES CONTAINING '-' ARE
        ##INCLUDED

        df['date_uuid']= df['date_uuid'].astype('string')
        df['date_uuid_check'] = df['date_uuid'].str.contains(r'^[a-zA-Z\d-]*$', na=False)
        df = df[df['date_uuid_check']==True]
        df = df.drop(['date_uuid_check'], axis=1)

        return df
       




        


        
        



    
        

        
                
                     

             
             
          

        

        


























