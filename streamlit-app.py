import streamlit as st
import json
import pandas as pd
import s3fs

FS = s3fs.S3FileSystem(anon=False, key=st.secrets['AWS_ACCESS_KEY_ID'], secret=st.secrets['AWS_SECRET_ACCESS_KEY'])
st.set_page_config(layout="wide")

@st.cache
def load(f1, f2):
    with FS.open(f1) as f:
        cluster_data = json.loads(f.read())
    with FS.open(f2) as f:
        df = pd.read_json(f, lines=True)
    
    v2count = []
    for i,c in enumerate(cluster_data):
        v2count.append({
            'cluster':i,
            'count':len(c['members'])
        })
        
    df_ = df.dropna(subset='similar_group_id')
    sims = df_[df_.similar_group_id.str.isnumeric()].similar_group_id.unique()
    count = df_.similar_group_id.value_counts().to_dict()
    sims = [s for s in sims if count[s]>1]
    return cluster_data, df, sims, df_.similar_group_id.value_counts(), pd.DataFrame(v2count).set_index('cluster')

def main():
    __cluster__ = 's3://qx-poc-public/docsim/results-15K.json'
    __all__ = 's3://qx-poc-public/docsim/results-15K-wholedf.json'
    cluster_data, df, sims, v1_counts, v2_counts = load(__cluster__, __all__)
    with st.sidebar:
        display = st.selectbox(
        'Display mode',
        ('V2 clusters', 'V1 clusters'))
        
        if display == 'V2 clusters':
            cluster = int(st.number_input(label='[V2 view only] Cluster number to display', min_value=0, max_value=len(cluster_data)))
            sim = None
        else:
            sim = st.selectbox(
                '[V1 view only] Choose similar_group_id to view items grouped by doc sim v1',
                sims
            )
            cluster = None

    if display=='V2 clusters':
        st.subheader('Displaying members and neighbors of clusters detected by Version 2')
        st.caption('20 Largest clusters')
        st.bar_chart(v2_counts.sort_values(by='count', ascending=False).head(20))
        st.caption('Cluster members')
        st.table(pd.DataFrame(cluster_data[cluster]['members'])[['title','content']])
        st.caption('Cluster neighbours')
        st.table(pd.DataFrame(cluster_data[cluster]['neighbours'])[['title','content']])
    else:
        st.subheader('Displaying members and neighbors of clusters detected by Version 1')
        st.caption('20 Largest clusters')
        st.bar_chart(v1_counts.sort_values(ascending=False).head(20))
        st.subheader('Displaying members of clusters detected by Version 1')
        st.table(df[df.similar_group_id==sim].head(300))

if __name__=='__main__': 
    main()
