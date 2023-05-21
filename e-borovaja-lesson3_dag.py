import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable



default_args = {
    'owner': 'e-borovaja-31',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 19),
}
schedule_interval = '0 12 * * *'



@dag(default_args=default_args, catchup = False)

def borovaja_game_stat():

    @task()
    def get_data(): # Считывает данные из файла и оставляет только тот год, который соответствует задани    

        link = 'https://drive.google.com/file/d/1dqesdMfFgoAX9gHfOmZ-sU5Rp66Hxbdr/view?usp=drivesdk'

        path = 'https://drive.google.com/uc?export=download&id='+link.split('/')[-2]

        Games = pd.read_csv(path)

        login = 'e-borovaja-31'
        year = 1994 + hash(f'{login}') % 23
        df = Games.query('Year == @year')
        return df



    @task()
    def get_game_sale(df):  # Какая игра была самой продаваемой в этом году во всем мире?

        game_sale = df.query('Rank == Rank.max()').Name.values[0]
        return game_sale



    @task()
    def get_genre_EU(df):  # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько

        gender_EU = df.groupby('Genre', as_index= False) \
                      .agg({'EU_Sales':'sum'}) \
                      .rename(columns={'EU_Sales':'Total_sales'}) \
                      .sort_values('Total_sales', ascending = False)

        gender_sale_EU = gender_EU.query('Total_sales == Total_sales.max()').Genre.values[0]
        return gender_sale_EU



    @task()
    def get_game_sale_platform(df):  # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом 
                            # в Северной Америке?
                            # Перечислить все, если их несколько
                            # В самих данных Sales - это и есть тираж. То есть там не деньги, а миллионы копий.


        million_sale_games = df.query('NA_Sales > 1.0') \
                               .groupby('Platform', as_index = False) \
                               .agg({'Name':'count'}) \
                               .rename(columns = {'Name':'count_of_games'}) \
                               .sort_values('count_of_games', ascending = False)

        game_sale_platform = million_sale_games.query('count_of_games == count_of_games.max()').Platform.values[0]
        return game_sale_platform



    @task()
    def get_japan_publisher(df):  # У какого издателя самые высокие средние продажи в Японии?
                             # Перечислить все, если их несколько

        japan_sale_publ = df.groupby('Publisher', as_index = False) \
                            .agg({'JP_Sales':'mean'}) \
                            .rename(columns = {'JP_Sales':'mean_JP_Sales'}) \
                            .sort_values('mean_JP_Sales', ascending = False)

        japan_publisher = japan_sale_publ.query('mean_JP_Sales == mean_JP_Sales.max()').Publisher.values[0]
        return japan_publisher



    @task()
    def get_EU_games_sale(df): # Сколько игр продались лучше в Европе, чем в Японии?

        EU_games_sale = df.query('EU_Sales > JP_Sales').Name.count()
        return EU_games_sale



    @task()
    def print_data(game_sale, gender_sale_EU, game_sale_platform, japan_publisher, EU_games_sale):
        date = ds

        print(f'''Data for {date}
              Game sale: {game_sale}
              Genre sale: {gender_sale_EU}
              Platform: {game_sale_platform}
              Japan publisher: {japan_publisher}
              Europe and Japan: {EU_games_sale}
              ''')


    df = get_data()
    game_sale = get_game_sale(df)
    gender_sale_EU = get_genre_EU(df)
    game_sale_platform = get_game_sale_platform(df)
    japan_publisher = get_japan_publisher(df)
    EU_games_sale = get_EU_games_sale(df)

    print_data(game_sale, gender_sale_EU, game_sale_platform, japan_publisher, EU_games_sale)

borovaja_game_stat = borovaja_game_stat()
