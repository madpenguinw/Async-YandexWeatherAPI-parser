import json
import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process

from api_client import YandexWeatherAPI
from utils import CITIES, GOOD_CONDITIONS

FORMAT = '%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
DATEFMT = '%Y-%m-%dT%H:%M:%S'

logging.basicConfig(
    format=FORMAT,
    datefmt=DATEFMT,
    level=logging.INFO,
)

formatter = logging.Formatter(
    FORMAT,
    datefmt=DATEFMT
)


logger = logging.getLogger(__name__)


class DataFetchingTask:
    '''
    Получение данных через АПИ Яндекс.Погоды.
    '''
    def get_yw_data(self, city: str) -> dict:
        logger.info(f'Начато получение данных для города {city}')
        yw_api = YandexWeatherAPI()
        yw_data: dict = yw_api.get_forecasting(city)
        logger.info(f'Закончено получение данных для города {city}')
        return yw_data


class DataCalculationTask(Process):
    '''
    Вычисление данных о погоде течение дня с 9 до 19 часов.
    '''
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def get_average_value(values: list) -> int:
        try:
            true_list = []
            for value in values:
                if value or str(value) == '0':
                    true_list.append(int(value))
            length = len(true_list)
            summary = sum(true_list)
            average = summary / length
        except ZeroDivisionError as error:
            logger.debug(error)
            return False
        except Exception as error:
            logger.error(error)
            return False
        return round(average, 2)

    def get_data_for_10_hours(city: str) -> dict:
        '''
        Парсинг данных о погоде с 9 до 19 часов.
        '''
        yw_object = DataFetchingTask()
        yw_data: dict = yw_object.get_yw_data(city)
        average_temp: list = list()
        average_not_rainy_hours: list = list()
        city_data: dict = dict()
        city_data['date_data']: list = list()
        city_data['city'] = city
        city_data = {
            'city': city,
            'date_data': [],
            'average_temp': float(),
            'average_not_rainy_hours': float(),
        }
        for day in yw_data['forecasts']:
            date = day['date']
            hours: list = day['hours']
            temperature_list: list = list()
            condition_counter: int = 0
            checking = False
            for hour_dict in hours:
                hour_int = int(hour_dict['hour'])
                if 9 <= hour_int <= 19:
                    temperature_list.append(hour_dict['temp'])
                    condition: str = hour_dict['condition']
                    checking = True
                    if condition in GOOD_CONDITIONS:
                        condition_counter += 1
            average_day_temp = DataCalculationTask.get_average_value(
                temperature_list)
            average_temp.append(average_day_temp)
            if checking:
                average_not_rainy_hours.append(condition_counter)
            if average_day_temp:
                day_data = {
                    'date': date,
                    'average_temp': average_day_temp,
                    'not_rainy_hours': condition_counter
                }
                city_data['date_data'].append(day_data)
        average_temp: float = DataCalculationTask.get_average_value(
            average_temp)
        average_not_rainy_hours: float = DataCalculationTask.get_average_value(
            average_not_rainy_hours)
        city_data['average_temp'] = average_temp
        city_data['average_not_rainy_hours'] = average_not_rainy_hours
        return city_data

    def run(self):
        with ThreadPoolExecutor() as pool:
            future = pool.map(
                DataCalculationTask.get_data_for_10_hours, CITIES.keys()
            )
            for city_data in future:
                self.queue.put(city_data)


class DataAggregationTask(Process):
    '''
    Объединение вычисленных данных.
    '''
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def run(self):
        data: list = list()
        while True:
            if self.queue.empty():
                logger.info('Объединение вычисленных данных выполненно')
                DataAnalyzingTask.create_json(data)
                return data
            item = self.queue.get()
            #  Продумать сравнение и рейтинговую систему
            data.append(item)


class DataAnalyzingTask:
    '''
    Финальный анализ и получение результата.
    '''
    def create_json(data):
        json_response = json.dumps(data, indent=4, ensure_ascii=False,)
        print(json_response)
        with open('data.json', 'w', encoding='utf-8') as outfile:
            outfile.write(json_response)
            outfile.write('\n')
