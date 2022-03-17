import json

from elasticsearch import Elasticsearch
from utils import backoff, work_with_state
from state import State, JsonFileStorage
from config import logging


logger = logging.getLogger('ElasticLoader')


class ElasticLoader:
    def __init__(self, host: list, path_file, table_name):
        self.client = Elasticsearch(host)
        self.state = State(JsonFileStorage(path_file))
        self.table_name = table_name

    @backoff(logger)
    def create_index(self, file_path) -> bool:
        """Создаёт index movies"""
        logger.info('Start create')
        with open(file_path, 'r') as file:
            f = json.load(file)
        if self.client.indices.exists(index="movies"):
            logger.info('Index movies already exist')
        self.client.index(index='movies', body=f)
        logger.info('End create')
        return True

    @backoff(logger)
    def load_data_bulk(self, movie_list) -> None:
        logger.info('Start load')
        self.client.bulk(body='\n'.join(movie_list) + '\n', index='movies', refresh=True)
        logger.info('End load')

        last_id = self.state.get_state('%s_temporary_id_film' % self.table_name)
        self.state.set_state('%s_last_id_film' % self.table_name, last_id)
        self.state.set_state('%s_temporary_id_film' % self.table_name, None)

        work_with_state(logger, self.state, self.table_name)
