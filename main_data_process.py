# Databricks notebook source
import json
import pandas as pd

def main_data_process():
    """
    This is to process data received from Kafka consumer as JSON list
    """
    json_dict = consumer_app()
    
    if isinstance(json_dict, dict):
        print("No data available at consumer. Rerun the whole process")
    
    else:
        json_data = json.loads(json_dict)
        
        df_user_data = pd.DataFrame(json_data)
        
        #Unique user count 
        
        print("1. Number of unique users: {}".format(len(df_user_data["id"].unique().tolist())))
        
        
        #Country counts
        
        df_user_county = df_user_data.groupby("country", as_index=False).agg({"id":"count"}).rename(columns = {"id":"country_count"})
        max_country = df_user_county[df_user_county.country_count == df_user_county.country_count.max()]["country"].iloc[0]
        min_country = df_user_county[df_user_county.country_count == df_user_county.country_count.min()]["country"].iloc[0]
        
        max_country_count = df_user_county[df_user_county.country_count == df_user_county.country_count.max()]["country_count"].iloc[0]
        min_country_count = df_user_county[df_user_county.country_count == df_user_county.country_count.min()]["country_count"].iloc[0]
        
        print("2. Most and least represted countries are {} ({} users) and {} ({} users) respectively.".format(max_country,max_country_count,min_country,min_country_count))
        
        
        #Gender Ratio for Top 5 most represented countries
        
        df_user_county_gender = df_user_data.groupby(["country","gender"]).agg({"id":"count"}).rename(columns = {"id":"gender_count"})
        #print(df_user_county_gender)
        df_gender_pivot = df_user_county_gender.pivot_table("gender_count",["country"],"gender").fillna(0)
        #print(df_gender_pivot)
        df_gender_pivot["total_user"] = df_gender_pivot["Female"] + df_gender_pivot["Male"]
        df_gender_pivot = df_gender_pivot.sort_values("total_user",ascending = False).head(5)       
        print("3. Top 5 countries with highest user participation:\n")
        print(df_gender_pivot)
        
    
        #Country with highest female participation
        
        df_female = df_user_data.loc[df_user_data["gender"] == "Female", ["id","country"]].groupby("country", as_index=False).agg({"id":"count"}).rename(columns = {"id":"female_count"})
        df_male = df_user_data.loc[df_user_data["gender"] == "Male", ["id","country"]].groupby("country", as_index=False).agg({"id":"count"}).rename(columns = {"id":"male_count"})
        df_female_male = pd.merge(df_female, df_male, on=["country"])
        df_female_male["female_percentage"] = df_female_male["female_count"]/(df_female_male["female_count"] + df_female_male["male_count"])
        
        max_country_female = df_female_male[df_female_male.female_percentage == df_female_male.female_percentage.max()]["country"].iloc[0]
        min_country_female = df_female_male[df_female_male.female_percentage == df_female_male.female_percentage.min()]["country"].iloc[0]
        
        max_female_percentage = round(df_female_male[df_female_male.female_percentage == df_female_male.female_percentage.max()]["female_percentage"].iloc[0]*100)
        min_female_percentage = round(df_female_male[df_female_male.female_percentage == df_female_male.female_percentage.min()]["female_percentage"].iloc[0]*100)
        print("\n4. Most and least represted countries by females are {} ({}%) and {} ({}%) respectively.".format(max_country_female,max_female_percentage,min_country_female,min_female_percentage))

        df_date_traffic = df_user_data[["ip_address","date"]]
        df =df_date_traffic.groupby([pd.to_datetime(df_date_traffic["date"]).dt.month,pd.to_datetime(df_date_traffic["date"]).dt.year])["ip_address"].count()
        df.rename(columns = {"date":"Month",1:"Year",2:"IP Count"},inplace =True)
        
        
        print(df)

main_data_process()
