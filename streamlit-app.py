import streamlit as st
import json
import pandas as pd
import s3fs
FS = s3fs.S3FileSystem(anon=False, key=st.secrets['AWS_ACCESS_KEY_ID'], secret=st.secrets['AWS_SECRET_ACCESS_KEY'])
st.set_page_config(layout="wide")

# @st.cache(allow_output_mutation=True)
def load(f1, f2):
    
    with FS.open(f1) as f:
        cluster_data = json.loads(f.read())
            
    with FS.open(f2) as f:
        df = pd.read_json(f, lines=True)
    df_ = df.dropna(subset='similar_group_id')
    sims = df_[df_.similar_group_id.str.isnumeric()].similar_group_id.unique()
    count = df_.similar_group_id.value_counts().to_dict()
    sims = [s for s in sims if count[s]>1]
    return cluster_data, df, sims

def main():
    cluster_data, df, sims = load(st.secrets['CLUSTER'], st.secrets['ALL'])
    with st.sidebar:
        display = st.selectbox(
        'Display mode',
        ('V2 clusters', 'V1 clusters'))

        cluster = int(st.number_input(label='[V2 view only] Cluster number to display', min_value=0, max_value=len(cluster_data)))

        sim = st.selectbox(
            '[V1 view only] Choose similar_group_id to view items grouped by doc sim v1',
            sims
        )

    if display=='V2 clusters':
        st.caption('Cluster members')
        st.table(pd.DataFrame(cluster_data[cluster]['members'])[['title','content']])
        st.caption('Cluster neighbours')
        st.table(pd.DataFrame(cluster_data[cluster]['neighbours'])[['title','content']])
    else:
        st.table(df[df.similar_group_id==sim].head(300))

if __name__=='__main__':
    main()
