
# coding: utf-8

# In[1]:

import numpy as np
import pandas as pd


# In[35]:

tweets = pd.read_csv('./data.csv')
tweets.head()
tweets.head()


# In[3]:

print tweets.columns.values

del tweets['longitude']
del tweets['latitude']

print tweets.columns.values


# In[4]:

print tweets.dtypes


# In[5]:

print tweets['id'].nunique()


# In[6]:

#tweets.groupby(tweets['screenName']).size()


# In[7]:

import matplotlib.pyplot as plt

replyCount = pd.DataFrame(tweets.groupby(tweets['replyToSN']).size().rename('tweetCount')).reset_index()

ind = np.arange(len(replyCount.index))  # the x locations for the groups
width = 1.0      # the width of the bars

plt.bar(ind, replyCount['tweetCount'], width )
plt.ylabel('Number of replies to')
plt.xlabel('User')
plt.xticks(ind + width/2, replyCount['replyToSN'], rotation='vertical')

fig_size = plt.rcParams["figure.figsize"]
fig_size[0] = 20
fig_size[1] = 10

plt.savefig('barchart.png')
plt.close()


# In[59]:

from datetime import datetime

hours = []

for strDate in tweets['created']:
    date = datetime.strptime(strDate, '%Y-%m-%d %H:%M:%S')
    hours.append(date.hour)

hours = pd.Series(np.array(hours))
tweets['hour'] = hours
tweetPerHour = pd.DataFrame(tweets.groupby(tweets['hour']).size().rename('tweetCount')).reset_index()
tweetPerHour['hour'][0] = 24 # Replace hour 0 by 24

hours = np.arange(1,25)
nbrTweets = []

for hour in hours:
    notInDF = True
    for i, row in enumerate(tweetPerHour['hour']):
        if row == hour:
            notInDF = False
            nbrTweets.append(tweetPerHour['tweetCount'][i])
            
    if notInDF:
        nbrTweets.append(0)
        

plt.plot(hours, nbrTweets)
plt.plot(hours, nbrTweets, 'ro')
plt.xlabel('Hours of day')
plt.ylabel('Numbers of tweet')
fig_size = plt.rcParams["figure.figsize"]
fig_size[0] = 10
fig_size[1] = 5

plt.savefig('linechart.png')


# In[16]:

from wordcloud import WordCloud

words = ''

for word in tweets['screenName']:
    words += word + " "

wordcloud = WordCloud(max_font_size=30).generate(words)


plt.imshow(wordcloud)
plt.axis("off")
plt.savefig('wordcloud.png')
plt.close()

