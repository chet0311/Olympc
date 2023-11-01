import pandas as pd
import numpy as np

# creating pandas datframe for ETL
athlete_df=pd.read_csv('athlete_events.csv')
regions_df=pd.read_csv('noc_regions.csv')

###      Part of Transformation    ###
# Analysing only summer Olympics
athlete_df=athlete_df[athlete_df['Season']=='Summer']
# merging two dataframes based on region 
result_df=athlete_df.merge(regions_df,on='NOC',how='left')
#renaming columns in result dataframe
result_df=result_df.rename(columns={'region': 'Region', 'notes': 'Notes'})
# dropping duplicate records
result_df.drop_duplicates(inplace=True)
result_df.shape
# creating dummy columns for medal
dummy=pd.get_dummies(result_df['Medal'])
dummy= dummy.astype(int)
#concating dummy dataframe with main dataframe
result_df=pd.concat([result_df,dummy],axis=1)

###         Part of Analytical Questions     #####
#Q Calculating Medal tally for each country in All Olympic seasons

# duplicating records for multiple medal for single event
Medal_tally=result_df.drop_duplicates(subset=['Team','Year','NOC','Sport','Games','Event','City','Medal'])
#converting region column into list
region=result_df['Region'].unique().tolist()
for reg in region:
    print(total_olmpc_medals(reg))

def total_olmpc_medals(country):

    Medals=Medal_tally.groupby('Region').sum()[['Gold','Silver','Bronze']].sort_values(by='Gold',ascending=False).reset_index()
    Medals=Medals[Medals['Region']==country]
    return Medals

#Q Calculating Medal tally for any country for any olympic season

## keeping count of medals one for one event and removing
result_df=result_df.drop_duplicates(subset=['Team','Year','NOC','Sport','Games','Event','City','Medal'])
def foreach(country,year):
    temp_df=result_df[(result_df['Region']==country )& (result_df['Year']==year)]
    eachseason=temp_df.groupby('Region').sum()[['Gold','Silver','Bronze']]
    eachseason['Total']=eachseason[['Gold', 'Silver', 'Bronze']].sum(axis=1)
    return eachseason

print(foreach('USA',2016))

#Q Calculating successful team in each olympic season

# convrting year column into list
years=result_df['Year'].unique().tolist()

for year in years:
    print(foreach(year))
    
def foreach(year):
    temp_df=result_df[(result_df['Year']==year)]
    eachseason=temp_df.groupby('Region').sum()[['Gold','Silver','Bronze']]
    eachseason['Total']=eachseason[['Gold', 'Silver', 'Bronze']].sum(axis=1)
    eachseason['Year']=year
    eachseason=eachseason[eachseason.Gold == eachseason.Gold.max()]
    return eachseason

#Q Calculating medal tally for any olympic year

def foreach(year):
    temp_df=result_df[(result_df['Year']==year)]
    each_olmp_year=temp_df.groupby('Region').sum()[['Gold','Silver','Bronze']]
    each_olmp_year['Total']=each_olmp_year[['Gold', 'Silver', 'Bronze']].sum(axis=1)
    each_olmp_year.sort_values(by=['Gold'], ascending=False)

    return each_olmp_year

print(foreach(2012))

#Q Calculating most successful Athlete in each Olympic season

for year in years:
    print(foreach(year))

def foreach(year):
    temp_df=result_df[(result_df['Year']==year)]
    eachseason=temp_df.groupby(['Region','Name']).sum()[['Gold','Silver','Bronze']]
    eachseason['Total']=eachseason[['Gold', 'Silver', 'Bronze']].sum(axis=1)
    eachseason['Year']=year
    index= eachseason['Gold'].idxmax()
    toprecord=eachseason.loc[index]
    return toprecord

#Q Calculating Most Successful Athlete in History of olympic

#overall Successful Athlete
Most_succss_Athlt=result_df.groupby(['Region','Name']).sum()[['Gold','Silver','Bronze']]
Most_succss_Athlt['Total']=Most_succss_Athlt[['Gold', 'Silver', 'Bronze']].sum(axis=1)
Most_succss_Athlt=Most_succss_Athlt[Most_succss_Athlt.Gold==Most_succss_Athlt.Gold.max()]
Most_succss_Athlt.tail()

# female successful athlete
Most_succss_Athlt_Female=result_df[(result_df['Sex']=='F')]

Most_succss_Athlt_Female=Most_succss_Athlt_Female.groupby(['Region','Name']).sum()[['Gold','Silver','Bronze']]
Most_succss_Athlt_Female['Total']=Most_succss_Athlt_Female[['Gold', 'Silver', 'Bronze']].sum(axis=1)
Most_succss_Athlt_Female=Most_succss_Athlt_Female[Most_succss_Athlt_Female.Gold==Most_succss_Athlt_Female.Gold.max()]
Most_succss_Athlt_Female.tail()

#Q Calculate yearwise participation of Male and Female

def participation(year,gender):
    participate=result_df[(result_df['Sex']==gender)&(result_df['Year']==year)]
    participate=participate.groupby(['Year','Sex']).size().reset_index(name='Count')

    return participate



# for Male yearwise participation
for year in years:
    print(participation(year,'M'))

# for Female yearwise participation
for year in years:
    print(participation(year,'F'))

# yearwise total athletes participation
def total_athletes(year):
    total_participate=result_df[(result_df['Year']==year)]
    total_participate=total_participate.groupby('Year').size().reset_index(name='Total_Participants')
    return total_participate

for year in years:
    print(total_athletes(year))