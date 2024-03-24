import requests
import pandas as pd
import numpy as np
from datetime import date
from config.config import CONFIG
from helpers.logger import error_logger

# OpenWeatherMap API


def ExtractWeatherData(locationlist, country):
    try:
        api_key = CONFIG.get('weather_API').get('api_key')

        base_url = "http://api.openweathermap.org/data/2.5/weather?"

        weather_list = []
        for city_name in locationlist:
            complete_url = base_url + "appid=" + api_key + "&q=" + city_name

            response = requests.get(complete_url)

            x = response.json()
            data = {}

            data['date'] = date.today()
            data['mintempc'] = x['main']['temp_min']
            data['maxtempc'] = x['main']['temp_max']
            data['pressure'] = x['main']['pressure']
            data['humidity'] = x['main']['humidity']
            data['visibility'] = x['visibility']
            data['precipmm'] = np.nan
            data['windspeedkmph'] = x['wind']['speed']
            data['location'] = city_name
            weather_list.append(data)

        weather_df = pd.DataFrame(weather_list)
        weather_df['date'] = pd.to_datetime(
            weather_df['date'], infer_datetime_format=True)
        weather_df['country'] = country
        weather_df2 = weather_df.copy()
        weather_df.drop(columns=['location'], inplace=True)
        weather_df = weather_df.groupby(
            by=['date', 'country'], as_index=False).mean()

        return weather_df2, weather_df

    except Exception as e:
        error_logger.exception(
            'Exception occurred while extracting weather data!')
