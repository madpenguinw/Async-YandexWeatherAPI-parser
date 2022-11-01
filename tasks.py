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
    """
    Получение данных через АПИ Яндекс.Погоды.
    """
    def get_yw_data(self, city: str) -> dict:
        logger.info('Getting data for city: %(city)s', {'city': city})
        yw_api = YandexWeatherAPI()
        yw_data: dict = yw_api.get_forecasting(city)
        logger.info('Finished getting data for city: %(city)s', {'city': city})
        return yw_data


class DataCalculationTask(Process):
    """
    Вычисление данных о погоде течение дня с 9 до 19 часов.
    """
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def get_average_value(self, values: list) -> int:
        'Получение среднего значения элемента списка.'
        try:
            true_list = []
            for value in values:
                # Необходимо, чтобы 0 обрабатывался, как число, а не как False
                if value or str(value) == '0':
                    true_list.append(int(value))
            length = len(true_list)
            summary = sum(true_list)
            average = summary / length
        except ZeroDivisionError as error:
            logger.debug(error)
            return False
        except TypeError as error:
            logger.error(error)
            return False
        return round(average, 2)

    def get_data_for_10_hours(self, city: str) -> dict:
        """
        Парсинг данных о погоде с 9 до 19 часов.
        """
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
            'rating': False,
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
            average_day_temp = self.get_average_value(
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
        average_temp: float = self.get_average_value(
            average_temp)
        average_not_rainy_hours: float = self.get_average_value(
            average_not_rainy_hours)
        city_data['average_temp'] = average_temp
        city_data['average_not_rainy_hours'] = average_not_rainy_hours
        return city_data

    def run(self):
        with ThreadPoolExecutor() as pool:
            future = pool.map(
                self.get_data_for_10_hours, CITIES.keys()
            )
            for city_data in future:
                self.queue.put(city_data)


class DataAggregationTask(Process):
    """
    Объединение вычисленных данных.
    """
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def get_rating(
        self, data: list, value: str, reverse: bool = False
    ) -> list:
        'Увеличение значения рейтинга на основании положения словаря в списке.'
        try:
            data.sort(
                key=lambda dictionary: dictionary[value],
                reverse=reverse
            )
            i = 1
            if value != 'rating':
                for sorted_dictionary in data:
                    if sorted_dictionary['rating']:
                        sorted_dictionary['rating'] += 1
                    else:
                        sorted_dictionary['rating'] = i
                        i += 1
        except KeyError as error:
            logger.error(error)
        return data

    def get_recommendation(self, data: list) -> str:
        'Сортировка списка словарей по ключу.'
        try:
            city_1 = data[0]['city']
            rating_1 = data[0]['rating']
            if data[1]['rating'] == rating_1:
                city_2 = data[1]['city']
                msg = 'Наиболее благоприятные города для поездки ' \
                    f'{city_1} и {city_2}'
            else:
                msg = f'Наиболее благоприятный город для поездки - {city_1}'
            print(msg)
            pass
        except LookupError as error:
            logger.error(error)
            pass

    def run(self):
        data: list = list()
        data_analyzing = DataAnalyzingTask()
        while True:
            if self.queue.empty():
                data = self.get_rating(
                    data,
                    'average_temp',
                    reverse=True
                )
                data = self.get_rating(
                    data,
                    'average_not_rainy_hours',
                    reverse=True
                )
                data = self.get_rating(
                    data,
                    'rating'
                )
                logger.info('Data aggregation is completed')
                data_analyzing.result(data)
                self.get_recommendation(data)
                return data
            item = self.queue.get()
            data.append(item)


class DataAnalyzingTask:
    """
    Финальный анализ и получение результата.
    """
    def create_json(self, data: list):
        'Создание из списка словарей объекта json.'
        dict_data = {
            'forecasting': data
        }
        json_data = json.dumps(dict_data, indent=4, ensure_ascii=False,)
        return json_data

    def save_json(self, json_data):
        'Cохранение объекта json в файл data.json.'
        with open('data.json', 'w', encoding='utf-8') as outfile:
            outfile.write(json_data)
            outfile.write('\n')
        logger.info('Result saved in file "data.json"')

    def result(self, data):
        'Конечный результат.'
        data_analyzing = DataAnalyzingTask()
        json_data = data_analyzing.create_json(data)
        data_analyzing.save_json(json_data)
