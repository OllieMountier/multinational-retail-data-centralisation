#%%
import pandas as pd
import tabula
import json
import requests
import smart_open
import numpy as np

import database_utils
import data_cleaning

head_dict={'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
number_of_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        
class DataExtractor:
    
    def read_rds_table(self, engine, table_name):
        return pd.read_sql_table(table_name, engine)
        
    def retrieve_pdf_data(self):
        df=tabula.read_pdf("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf",pages='all')
        data=pd.concat(df,ignore_index=True)
        df2 = pd.DataFrame(data)
        return df2
    
    def list_stores_data(self, endpoint, header):
        response = requests.get(endpoint, headers=header)
        print(response.content)
        

    def retrieve_stores_data(self, endpoint, header):
        store_number = 0
        store_dataframe = pd.DataFrame(columns = ['index', 'address', 'longitude', 'lat', 'locality', 'store_code', 'staff_numbers', 'opening_date', 'store_type', 'latitude', 'country_code', 'continent'])
        while store_number < 451:
            full_store_endpoint = endpoint + str(store_number)
            response = requests.get(full_store_endpoint, headers=header)
            store_data = response.content
            store_data_dictionary = json.loads(store_data.decode('utf-8'))
            new_row = pd.DataFrame([store_data_dictionary], columns=['index', 'address', 'longitude', 'lat', 'locality', 'store_code', 'staff_numbers', 'opening_date', 'store_type', 'latitude', 'country_code', 'continent'])
            store_dataframe = pd.concat([store_dataframe, new_row], ignore_index='True')
            full_store_endpoint = endpoint
            store_number = store_number + 1
        return store_dataframe   

    def extract_from_s3(self, address):
         products_dataframe = pd.read_csv(smart_open.smart_open(address))
         return products_dataframe
    
    def extract_date_details(self, address):
        date_details = pd.read_json(smart_open.smart_open(address))
        return date_details
       

if __name__ == "__main__":
    dbc = database_utils.DatabaseConnector()       
    de = DataExtractor()
    dc = data_cleaning.DataCleaner()
    
    table_names = dbc.list_db_tables()
    
    #user_dataframe = de.read_rds_table(dbc.engine, table_names[1])
    #cleaned_users = dc.clean_user_data(user_dataframe)
    
    #table = 'dim_user'
    #uploaded_df = dbc.upload_to_db(cleaned_users, table)
    
    #card_details = de.retrieve_pdf_data()
    #cleaned_cards= dc.clean_card_data(card_details)
    
    #table = 'dim_card_details'
    #uploaded_card_details = dbc.upload_to_db(cleaned_cards, table)
    
    #de.list_stores_data(number_of_store_endpoint, head_dict)
    #store_dataframe = de.retrieve_stores_data(retrieve_store_endpoint, head_dict)
    #cleaned_store = dc.clean_store_data(store_dataframe)
    
    #table = 'dim_store_details'
    #uploaded_store_details = dbc.upload_to_db(cleaned_store, table)

    #products_dataframe = de.extract_from_s3('s3://data-handling-public/products.csv')
    #cleaned_weight = dc.convert_products_weight(products_dataframe)
    #cleaned_products = dc.clean_products_data(cleaned_weight)
    
    #table = 'dim_products'
    #uploaded_products = dbc.upload_to_db(cleaned_products, table)

    #orders_data = de.read_rds_table(dbc.engine, table_names[2])
    #cleaned_orders = dc.clean_orders_data(orders_data)
    
    #table = 'orders_table'
    #uploaded_orders = dbc.upload_to_db(cleaned_orders, table)

    date_details = de.extract_date_details('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    #print(date_details['timestamp'])
    cleaned_dates = dc.clean_date_details(date_details)
    #print(cleaned_dates['timestamp'])
    #print(cleaned_dates.info())

    table = 'dim_date_times'
    uploaded_date = dbc.upload_to_db(cleaned_dates, table)
        




# %%
