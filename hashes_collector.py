import os
import hashlib
from tqdm import tqdm
import pandas as pd
import psycopg2

conn = psycopg2.connect("""
    host=host
    port=port
    sslmode=require
    dbname=dbname
    user=user
    password=password
    target_session_attrs=read-write
""")

q = conn.cursor()
hash_dict = {}
full_df = pd.DataFrame()
sets_dir_path = ''

# COLLECT ALL HASHES FROM SETS
for root, subdirectories, files in os.walk(sets_dir_path):
    for subdirectory in subdirectories:
        pass
    for file in files:
        print(os.path.join(root, file))
        hash = hashlib.md5(open(os.path.join(root, file), 'rb').read()).hexdigest()
        assignment_id = os.path.join(root, file).split('\\')[-2]
        file_name = os.path.join(root, file).split('\\')[-1]
        print(assignment_id, ' ', file_name, ' ', hash)
        df = pd.DataFrame(data={'assignment_id':[assignment_id], 'file_name':[file_name], 'hash':[hash]})
        full_df = pd.concat([full_df, df])

full_df.to_csv('hashes.tsv', sep='\t', index=False)

# #############################################################################################################################
df = pd.read_csv('hashes.tsv', sep='\t')

# SET UP ALL FILES HASHES FROM ONE SET TO ONE DICT
for i in tqdm(df['assignment_id'].unique()):
    df_one_set = df[df['assignment_id']==i]
    assignment_id = i
    set_hashes = {}
    for file_name in df_one_set['file_name']:
        set_hashes[file_name] = df_one_set[df_one_set['file_name']==file_name]['hash'].values[0]
    df1 = pd.DataFrame(data={'assignment_id':[assignment_id], 'files_hashes':[set_hashes]})
    full_df = pd.concat([full_df, df1])

full_df.to_csv('files_hashes_to_db.tsv', sep='\t', index=False)
# ############################################################################################################################
df = pd.read_csv('files_hashes_to_db.tsv', sep='\t')

# SAVE HASHES TO DATABASE
for i in tqdm(df['assignment_id']):
    print(i)
    hashes = df[df['assignment_id']==i]['files_hashes'].values[0].replace("'", '"')
    update_query = f"UPDATE public.sets SET hashes = '{hashes}' WHERE assignment_id = '{i}';"
    q.execute(update_query)

conn.commit()

conn.close()