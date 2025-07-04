import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import pandas as pd
import psycopg2
from nltk.corpus import stopwords
import nltk
from dotenv import load_dotenv
import os

# Download NLTK stopwords
nltk.download('stopwords', quiet=True)

load_dotenv()

# Connection string (DSN format)
conn_str =  os.environ['CONNECTION_STRING']

# Connect and load data
conn = psycopg2.connect(conn_str)
query = "SELECT * FROM articles;"
data = pd.read_sql(query, conn)

conn.close()
data.columns = ['_'.join(word.capitalize() for word in col.split('_')) for col in data.columns]
# Streamlit page configuration
st.set_page_config(page_title="Blog Dashboard", layout="wide")
st.title("Interactive Article Insights Dashboard")
st.write("Explore and interact with article data from our database.")

# Data preprocessing
data['Time_Uploaded'] = pd.to_datetime(data['Time_Uploaded'], utc=True)
data['Word_Count'] = data['Word_Count'].fillna(0)

# Sidebar filters
st.sidebar.title("Filters")

author = st.sidebar.selectbox('Select Author', options=['All'] + list(data['Authors'].unique()))
sentiment = st.sidebar.multiselect('Select Sentiment', options=data['Sentiment'].unique(), default=data['Sentiment'].unique())
all_tags = set(tag for tags in data['Tags'].str.split('#') for tag in tags if tag)
tags = st.sidebar.multiselect('Select Tags', options=list(all_tags))

# Date range filter
earliest_date = data['Time_Uploaded'].min().date()
latest_date = data['Time_Uploaded'].max().date()
min_date, max_date = st.sidebar.date_input(
    "Select Date Range",
    [earliest_date, latest_date],
    min_value=earliest_date,
    max_value=latest_date
)
min_date, max_date = pd.to_datetime(min_date).tz_localize('UTC'), pd.to_datetime(max_date).tz_localize('UTC')

# Word count and reading time filters
min_word_count, max_word_count = st.sidebar.slider(
    "Word Count Range", 
    min_value=int(data['Word_Count'].min()), 
    max_value=int(data['Word_Count'].max()), 
    value=(int(data['Word_Count'].min()), int(data['Word_Count'].max()))
)
min_reading_time, max_reading_time = st.sidebar.slider(
    "Reading Time Range (minutes)", 
    min_value=0, 
    max_value=int(data['Reading_Time'].max()), 
    value=(0, int(data['Reading_Time'].max()))
)

# Apply filters
if author != 'All':
    data = data[data['Authors'] == author]
if sentiment:
    data = data[data['Sentiment'].isin(sentiment)]
if tags:
    data = data[data['Tags'].apply(lambda x: any(tag in x for tag in tags))]
data = data[
    (data['Time_Uploaded'] >= min_date) & 
    (data['Time_Uploaded'] <= max_date) &
    (data['Word_Count'] >= min_word_count) & 
    (data['Word_Count'] <= max_word_count) &
    (data['Reading_Time'] >= min_reading_time) & 
    (data['Reading_Time'] <= max_reading_time)
]

# Display filtered data
st.subheader(f"Showing {len(data)} Articles")
st.dataframe(data[['Title', 'Authors', 'Time_Uploaded', 'Tags', 'Sentiment', 'Article_Content']])

# Visualizations
col1, col2 = st.columns(2)

with col1:
    st.subheader("Sentiment Distribution")
    fig, ax = plt.subplots()
    custom_palette = {'Positive': 'green', 'Neutral': 'grey', 'Negative': 'red'}
    sns.countplot(data=data, x='Sentiment', ax=ax, palette=custom_palette, dodge=False)
    st.pyplot(fig)

with col2:
    st.subheader("Reading Time Distribution")
    fig, ax = plt.subplots()
    sns.histplot(data['Reading_Time'], bins=10, ax=ax, kde=True)
    st.pyplot(fig)

st.subheader("Most Common Tags")
tags_list = data['Tags'].str.split('#').explode()
top_tags = tags_list.value_counts().head(10)
st.bar_chart(top_tags)

st.subheader("Word Count by Sentiment")
fig, ax = plt.subplots()
sns.boxplot(x='Sentiment', y='Word_Count', data=data, ax=ax, hue='Sentiment', palette='Set2', dodge=False, legend=False)
st.pyplot(fig)

st.subheader("Word Cloud of Tags")
tags_string = ' '.join(tags_list.dropna().values)
wordcloud = WordCloud(background_color='white', width=800, height=400).generate(tags_string)
fig, ax = plt.subplots()
ax.imshow(wordcloud, interpolation='bilinear')
ax.axis('off')
st.pyplot(fig)

st.subheader("Content Upload Distribution by Hour of the Day")
data['Upload Hour'] = data['Time_Uploaded'].dt.hour
fig, ax = plt.subplots()
sns.countplot(data=data, x='Upload Hour', ax=ax, palette='viridis', order=range(24))
ax.set_xticks(range(24))
ax.set_xticklabels([f'{hour}:00' for hour in range(24)], rotation=45)
ax.set_xlabel("Hour of the Day")
ax.set_ylabel("Number of Articles")
st.pyplot(fig)

st.subheader("Word Cloud of Article Content (Without Stopwords)")
stop_words = set(stopwords.words('english'))
text = ' '.join(data['Article_Content'].dropna())
wordcloud = WordCloud(stopwords=stop_words, background_color='white', width=800, height=400).generate(text)
fig, ax = plt.subplots(figsize=(10, 5))
ax.imshow(wordcloud, interpolation='bilinear')
ax.axis('off')
st.pyplot(fig)

st.subheader("Additional Statistics")
st.write("**Average Word Count**:", data['Word_Count'].mean())
st.write("**Average Reading Time**:", data['Reading_Time'].mean())
st.write("**Total Articles**:", len(data))