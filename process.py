import time

import psycopg2
from contextlib import closing
from psycopg2.extras import DictCursor

from config import dsn, elastic_conf, logging
from models import FilmWorkModel
from load_from_postgres import PostgresLoader
from load_to_elastic import ElasticLoader
from utils import backoff, rows_to_tuple, add_movie_in_list


logger = logging.getLogger('Start ETLProcess')


class ETLProcess:
    def __init__(self, dsn, elastic_conf):
        self.dsn = dsn
        self.elastic_conf = elastic_conf
        self.raw_data = None
        self.elastic_data = None
        self.index_created = False
        self.count_raws_data = 0
        self.list_table = ['person', 'genre', 'film_work']

    @backoff(logger)
    def extract(self, table) -> None:
        """Получение данных из Postgres"""
        with closing(psycopg2.connect(**self.dsn, cursor_factory=DictCursor)) as pg_conn:
            logger.info('PostgreSQL connection is open. Start load data')
            postgres_loader = PostgresLoader(pg_conn, path_file='volumes/State_%s.txt' % table, table_name=table)
            self.count_raws_data, self.raw_data = postgres_loader.loader_raw_data()

    def transform(self) -> None:
        """Обрабатывает сырые данные и преобразовывает в формат,
        пригодный для Elasticsearch"""

        logger.info('Start transformation raw data')
        data = []
        for row in self.raw_data:
            obj = FilmWorkModel(
                id=dict(row).get('id'),
                imdb_rating=dict(row).get('rating'),
                genre=rows_to_tuple(dict(row).get('genre')),
                title=dict(row).get('title'),
                description=dict(row).get('description'),
                director=dict(row).get('director'),
                actors_names=dict(row).get('actors_names'),
                writers_names=dict(row).get('writers_names'),
                actors=dict(row).get('actors'),
                writers=dict(row).get('writers'),
            ).dict()
            add_movie_in_list(data, obj)
        logger.info('Preparing data for loading into Elasticsearch - Success!')
        self.elastic_data = data

    def load(self, table) -> None:
        """Загружает данные в Elasticsearch"""
        logger.info('Start load data in Elastic')
        elastic_loader = ElasticLoader(self.elastic_conf, path_file='volumes/State_%s.txt' % table, table_name=table)
        if not self.index_created:
            self.index_created = elastic_loader.create_index('es_schema.json')
        elastic_loader.load_data_bulk(self.elastic_data)
        logger.info('Uploading data to Elastic was Successful!!!')

    def run(self) -> None:
        """Запуск архитектуры ETL, без остановки"""
        logger.info('Run ETLProcess')
        while True:
            logger.info('Start of while loop')
            for table in self.list_table:
                logger.info('Execute Work with table %s' % table)
                self.extract(table)
                if self.count_raws_data == 0:
                    continue
                self.transform()
                self.load(table)
                time.sleep(5)
            time.sleep(10)


if __name__ == '__main__':
    etl_process = ETLProcess(dsn, elastic_conf)
    etl_process.run()
