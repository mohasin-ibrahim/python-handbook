import os
import pandas as pd
import json

# Loading the entire json as object
with open(os.getcwd()+"/python-utils/files/nyphil.json") as f:
    df = json.load(f)

# Creating empty lists inorder to append each row after normalizing
df1 = []
df2 = []
df3 = []
df4 = []

#  Creating programs, works, soloists dataframes through loop
for program in df['programs']:
    if(program['id'] == '00646b9f-fec7-4ffb-9fb1-faae410bd9dc-0.1'):
        program_df = pd.json_normalize(program)
        id = program['id']
        df3.append(program_df)
        for work in program['works']:
            work_df = pd.json_normalize(work)
            work_df['id'] = id
            ID = work_df['ID']
            df1.append(work_df)
            if (len(work['soloists']) == 0):
                solo_ = pd.DataFrame()
                solo_['id'] = work_df['id']
                solo_['ID'] = work_df['ID']
                df2.append(solo_)
            else:
                for soloist in work['soloists']:
                    solo_df = pd.json_normalize(soloist)
                    solo_df['id'] = work_df['id']
                    solo_df['ID'] = work_df['ID']
                    df2.append(solo_df)

# Creating concerts only dataframe
# This processing can also be added in the previous loop as well.
for program in df['programs']:
    if(program['id'] == '00646b9f-fec7-4ffb-9fb1-faae410bd9dc-0.1'):
        id = program['id']
        for concert in program['concerts']:
            concert_df = pd.json_normalize(concert)
            concert_df['id'] = program['id']
            df4.append(concert_df)

# Concatenating the list to form final dataframes for program, work, concerts, soloists
p_df = pd.concat(df3, sort=False)
p_df.drop(['concerts', 'works'], axis=1, inplace=True)
w_df = pd.concat(df1, sort=False)
w_df.drop(['soloists'], axis=1, inplace=True)
s_df = pd.concat(df2, sort=False)
c_df = pd.concat(df4, sort=False)

# Merging dataframes one by one
out_1 = w_df.merge(s_df, how="inner", left_on=['id', 'ID'], right_on=['id', 'ID'])
out_2 = out_1.merge(c_df, how="inner", left_on=['id'], right_on=['id'])
out_3 = out_2.merge(p_df, how="inner", left_on=['id'], right_on=['id'])

# Saving all the dataframes separately as well as the output
p_df.to_csv(os.getcwd()+"/python-utils/files/p_df.csv", index=False)
w_df.to_csv(os.getcwd()+"/python-utils/files/w_df.csv", index=False)
s_df.to_csv(os.getcwd()+"/python-utils/files/s_df.csv", index=False)
c_df.to_csv(os.getcwd()+"/python-utils/files/c_df.csv", index=False)
out_3.to_csv(os.getcwd()+"/python-utils/files/out.csv", index=False)
