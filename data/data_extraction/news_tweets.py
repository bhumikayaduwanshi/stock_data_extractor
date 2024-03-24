from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd
from datetime import date, timedelta
import regex as re
from newspaper import Article
import tweepy as tw
from bs4 import BeautifulSoup
from requests_html import HTMLSession
from config.config import CONFIG
from helpers.logger import error_logger


def getTweets(company_name, until=date.today() + timedelta(days=1)):
    # add 1 day in today's date to include today's data in final dataframe
    consumer_key = CONFIG.get('twitter_API').get('consumer_key')
    consumer_secret = CONFIG.get('twitter_API').get('consumer_secret')
    access_token = CONFIG.get('twitter_API').get('access_token')
    access_token_secret = CONFIG.get('twitter_API').get('access_token_secret')

    '''bearer_token = 'AAAAAAAAAAAAAAAAAAAAAHuRnAEAAAAAX1SU8PGnX%2BE8cccRVCDEra%2FYZ7w
                             %3DhV1627VRMtQxkwNRppj3H1kCuFqGvSzgOg260WkXYGUu9Givez'''

    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)

    filteredTweets = []

    def filtertweets(x):
        x = x.lower()
        x = re.sub('[^ a-zA-Z0-9]', '', x)
        return x

    tweets = tw.Cursor(api.search_tweets,
                       q=company_name,
                       lang="en",
                       until=until).items(20)
    try:
        for tweet in tweets:
            date = tweet.created_at.date()
            tweet = filtertweets(tweet.text)
            filteredTweets.append([date, tweet])

        tf = pd.DataFrame(filteredTweets, columns=['date', 'tweets'])
        tf.drop_duplicates(inplace=True, keep='last', ignore_index=True)
        tf.insert(0, 'asset_id', company_name)
        return tf
    except Exception as e:
        error_logger.exception('Exception occurred while tweets extraction!')

# Fetch News For Stock


def ExtractNewsData(company_name, symbol, start_date=date.today(), end_date=date.today()):
    start_date = start_date-timedelta(days=1)
    month, day, year = str(start_date.month), str(
        start_date.day), str(start_date.year)
    month_1, day_1, year_1 = str(end_date.month), str(
        end_date.day), str(end_date.year)
    news_data = []
    session = HTMLSession()
    page = session.get(
        f"https://www.google.com/search?q={company_name.lower()}&rlz=1C1CHBF_enIN1002IN1003&tbas=0&biw=1422&bih=677&source=lnt&tbs=cdr%3A1%2Ccd_min%3A{month}%2F{day}%2F{year}%2Ccd_max%3A{month_1}%2F{day_1}%2F{year_1}&tbm=nws")
    page.html.render(timeout=50)
    soup = BeautifulSoup(page.html.html, 'lxml')
    body = soup.find_all(class_="WlydOe")
    for element in body:
        link = (element.get('href'))
        # date = (element.find(class_='OSrXXb').span.text)
        news_data.append([link])
    df = pd.DataFrame(news_data, columns=["Link"])
    df["Link"] = df["Link"].str.replace(" ", "")
    article_list = list()
    try:
        for i in df.index:
            article_dict = dict()
            article = Article('https://' + df['Link'][i] if df['Link']
                              [i].startswith('news.google.com') else df['Link'][i])
            try:
                article.download()
                article.parse()
                article.nlp()
            except Exception:
                continue

            # Storing results in our empty dictionary
            article_dict['date'] = end_date
            # article_dict['Title'] = article.title
            # article_dict['Article'] = article.text
            article_dict['news'] = article.summary
            # article_dict['Key_words'] = article.keywords

            article_list.append(article_dict)

        check_empty = not any(article_list)

        if check_empty == False:
            news_df = pd.DataFrame(article_list)
            news_df.insert(0, 'asset_id', symbol)
            return news_df
        else:
            news_df = pd.DataFrame(columns=["asset_id", "date", "news"])
            return news_df

    except Exception as e:
        error_logger.exception('Exception occured while extracting news data!')
        news_df = pd.DataFrame(columns=["asset_id", "date", "news"])
        return news_df

# Get Sentiment Analysis on News and Tweets


def getSentiment(df, col_name, measurement="compound"):
    sia = SentimentIntensityAnalyzer()
    if df.empty == False:
        try:
            df.columns = df.columns.str.lower()
            symbol = df['asset_id'].iloc[0]
            df['sentiment'] = df[col_name].apply(
                lambda x: sia.polarity_scores(x)[measurement])

            # Creating a DF with the average sentiment score each day
            sent_df = df.groupby('date')['sentiment'].mean().reset_index()
            sent_df = sent_df.loc[sent_df['date'] == date.today()]
            sent_df = sent_df.reset_index(drop=True)
            sent_df.insert(0, 'asset_id', symbol)
            sent_df.rename(
                columns={'sentiment': f'{col_name}_sentiment'}, inplace=True)
            return sent_df
        except Exception as e:
            error_logger.exception('Sentiment calculation not done!')
            return pd.DataFrame(columns=['asset_id', 'date', f'{col_name}_sentiment'])
    else:
        return pd.DataFrame(columns=['asset_id', 'date', f'{col_name}_sentiment'])
