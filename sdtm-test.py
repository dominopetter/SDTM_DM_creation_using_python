#!/usr/bin/env python
# coding: utf-8

# # SDTM DM dataset creation using Python
# This program can create SDTM DM SAS xpt dataset from EDC raw SAS datasets. 

# In[2]:


### Import modules
from pandas import Series, DataFrame
import pandas as pd
import xport  ## write sas datasets to local drive
from sas7bdat import SAS7BDAT  ## read SAS datasets from local drive


# ## Prepare demog datasets

# In[3]:


pd.read_sas('data/Raw/demog.xpt', encoding="utf-8")


# In[4]:


df_dm2 = pd.read_sas('data/Raw/demog.xpt', encoding="utf-8")


# In[5]:


### Prepare variables 
df_dm2['SEX'] = df_dm2.SEXC.replace(['Male','Female'], ['M','F']) # Create SEX variable
df_dm2['DOMAIN'], df_dm2['COUNTRY'] = 'DM', 'USA' # Create varialbes DOMAIN & COUNTRY
df_dm2['USUBJID'] = df_dm2.STUDYID + '-' + df_dm2.SITEID + '-' + df_dm2.SUBJID  # create USUBJID

for index, _df1 in df_dm2.iterrows():
    ## Create Ethnic
    if _df1['RACEC'].__contains__('HISPANIC'):
        df_dm2.loc[index, 'ETHNIC'] = 'HISPANIC OR LATINO'
    else:
        df_dm2.loc[index, 'ETHNIC'] = 'NOT HISPANIC OR LATINO'
        
    ## Create Race    
    if _df1['RACEC'].__contains__('HISPANIC'):
        df_dm2.loc[index, 'RACE'] = 'WHITE'
    elif _df1['RACEC'].__contains__('CAUCASIAN'):
        df_dm2.loc[index, 'RACE'] = 'WHITE'
    elif _df1['RACEC'].__contains__('AFRICAN'):
        df_dm2.loc[index, 'RACE'] = 'BLACK'
    elif _df1['RACEC'].__contains__('OTHER'):
        df_dm2.loc[index, 'RACE'] = 'OTHER'


# ## Prepare Exposure dataset

# In[6]:


df_ex2 = pd.read_sas('data/Raw/expo.xpt', encoding="utf-8")


# In[7]:


### Prepare exposure variables
df_ex2['EXSTDTC'] = df_ex2['EXSTDD'] + '-' + df_ex2['EXSTMM'] + '-' + df_ex2['EXSTYY']  # exposure start date
df_ex2['EXENDTC'] = df_ex2['EXENDD'] + '-' + df_ex2['EXENMM'] + '-' + df_ex2['EXENYY']  # exposure end date

### Fine the first and last exposure date
df_ex4 = df_ex2.sort_values(by =['SUBJID','EXSTDTC'], ascending=[True, True] )  # sort by subjid and expo date
df_ex4_f = df_ex4.groupby('SUBJID').first()  # pick the first exposure date of the subject
df_ex4_f2 = df_ex4_f.reset_index()  # reset index so that SUBJID is column
df_ex4_f3 = df_ex4_f2[['SUBJID','EXSTDTC']].rename(columns={'EXSTDTC':'RFSTDTC'})  # select variables
df_ex4_l = df_ex4.groupby('SUBJID').last()  # pick the last exposure date of the subject
df_ex4_l2 = df_ex4_l.reset_index()  # reset index so that SUBJID is column
df_ex4_l3 = df_ex4_l2[['SUBJID','EXENDTC']].rename(columns={'EXENDTC':'RFENDTC'})  # select variables


# ### merge exposure data to demog data

# In[8]:


### merge first and last exposure date
df_ex5 = pd.merge(df_ex4_f3, df_ex4_l3, on='SUBJID')

### merge exposure data to demog data
df_dm3 = pd.merge(df_dm2, df_ex5, on='SUBJID', how='left')  # merge exposure information to dm


# ## Prepare Randomization data

# In[9]:


df_rm2 = pd.read_sas('data/Raw/rand.xpt', encoding="utf-8")


# ### merge randomization data to demog data

# In[10]:


### Merge(inner join) randomization with demo by SUBJID. 
df_dm4 = pd.merge(df_dm3, df_rm2, on='SUBJID', how='left')
df_dm4['ARM'] = df_dm4.DRUG
df_dm4['ARMCD'] = df_dm4.ARM.replace(['Control','Study Drug'], ['C','SD'])
df_dm4['ACTARMCD'] = df_dm4.ARMCD
df_dm4['ACTARM'] = df_dm4.ARM


# ## Prepare Dispostion data

# In[11]:


df_ds2 = pd.read_sas('data/Raw/disp.xpt', encoding="utf-8")


# In[12]:


### Prepare dispostion data
df_ds2['DSSTDTC'] = df_ds2['DSSTDD'] + '-' + df_ds2['DSSTMM'] + '-' + df_ds2['DSSTYY']  # dispostion date

### prepare death data
for index3, _df3 in df_ds2.iterrows(): 
    if _df3.DISP == 'DEATH':
        df_ds2.loc[index3, 'DTHDTC'] = _df3.DSSTDTC
        df_ds2.loc[index3, 'DTHFL'] = 'Y'
        
df_ds2['RFPENDTC'] = df_ds2.DSSTDTC
df_ds4 = df_ds2[['SUBJID', 'RFPENDTC', 'DTHFL', 'DTHDTC']]


# ### Merge dispostion data to demog data

# In[13]:


df_dm5 = pd.merge(df_dm4, df_ds4, on='SUBJID', how='left')


# In[14]:


### Prepare demographic data
df_dm6 = df_dm5.drop(['SEXC','RACEC','DRUG'],1)  # drop sex variable
df_dm6['RFXSTTDTC'], df_dm6['RFSENDTC'], df_dm6['RFICDTC'] = df_dm6.RFSTDTC, df_dm6.RFENDTC, ''  


# ## Write CDISC DM dataset

# In[15]:


df_dm6 = df_dm6.rename(columns={"RFXSTTDTC":"RFXSTTDT"})

with open('data/CDISC/dm.xpt', 'wb') as f:
    xport.from_dataframe(df_dm6, f)## write DM 


# In[ ]:




