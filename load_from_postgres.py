from dateutil.parser import parse
from psycopg2 import sql

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from state import JsonFileStorage, State
from config import array_agg_request, logging
from utils import rows_to_tuple, work_with_state


logger = logging.getLogger('PostgresLoader')


class PostgresLoader:
    """Выгрузка данных из postgres"""
    def __init__(self, pg_conn: _connection, path_file, table_name):
        self.conn = pg_conn
        self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        self.state = State(JsonFileStorage(path_file))
        self.table_name = table_name
        self.batch_size = 100

        self.list_last_row = False
        self.last_film_id = False
        self.end_film_id = False
        self.load_list_id_count_zero = False

    def load_list_id(self) -> str:
        """Вложенный запрос на получение списка id измененных данных связанных таблиц"""
        logger.info('Start load')

        list_value = {}
        updated_at = self.state.get_state('%s_last_update_at' % self.table_name)
        last_id = self.state.get_state('%s_last_id' % self.table_name)

        if updated_at is not None:
            where = sql.SQL('''WHERE 
                                updated_at  = %(updated_at)s and id > %(id)s
                                or updated_at > %(updated_at)s
                                ''')

            list_value['updated_at'] = parse(updated_at)
            list_value['id'] = last_id
        else:
            where = sql.SQL(''' ''')

        list_id_sql = sql.SQL('''SELECT id, updated_at
                            FROM content.{table_name}
                            {where}
                            ORDER BY updated_at, id
                            LIMIT %(batch_size)s; 
                            ''')\
            .format(table_name=sql.Identifier(self.table_name), where=where)

        list_value['batch_size'] = self.batch_size

        self.cursor.execute(list_id_sql, list_value)
        list_id = self.cursor.fetchall()
        if self.cursor.rowcount == 0:
            logger.info('Changed data not found')
            self.load_list_id_count_zero = True
        else:
            self.list_last_row = list_id[-1]
            logger.info('Add to the state temporarily id and update_at of the last row of changed elements')
            self.state.set_state('%s_temporary_id' % self.table_name, self.list_last_row[0])
            self.state.set_state('%s_temporary_update_at' % self.table_name, self.list_last_row[1].isoformat())
        logger.info('End load')
        return list_id

    def load_film_work_id(self) -> list:
        """Вложенный запрос на получение id фильмов
        у которых изменились данные в связанных таблицах"""
        logger.info('Start load')

        last_id = self.state.get_state('%s_last_id_film' % self.table_name)
        list_value = {}

        if last_id:
            and_where = sql.SQL('''and  fw.id > %(id)s::uuid''').format(id=last_id)
            list_value['id'] = last_id
        else:
            and_where = sql.SQL(''' ''')

        load_film_id_sql = sql.SQL('''SELECT DISTINCT fw.id
                            FROM content.film_work fw
                            LEFT JOIN content.{table_name_fw} tfw ON tfw.film_work_id = fw.id
                            WHERE tfw.{table_name_id} IN %(list_id)s
                            {and_where}
                            ORDER BY fw.id
                            LIMIT %(batch_size)s;
                            ''')\
            .format(
            table_name_fw=sql.Identifier(self.table_name + '_film_work'),
            table_name_id=sql.Identifier(self.table_name + '_id'),
            and_where=and_where)
        list_value['list_id'] = rows_to_tuple(self.load_list_id())
        list_value['batch_size'] = self.batch_size
        if self.load_list_id_count_zero:
            list_film_id = []
            logger.info('Changed data not found')
        else:
            self.cursor.execute(load_film_id_sql, list_value)

            list_film_id = self.cursor.fetchall()
            if self.cursor.rowcount == 0:
                logger.info('Changed data not found')
                self.end_film_id = True
                self.state.set_state('%s_end_film_id' % self.table_name, self.end_film_id)
                work_with_state(logger, self.state, self.table_name)

            else:
                self.last_film_id = list_film_id[-1][0]

                logger.info('Add to the state temporarily the id of the last movie,'
                            'so next time you can get the next 100 rows')
                self.state.set_state('%s_temporary_id_film' % self.table_name, self.last_film_id)

                if self.cursor.rowcount < 100:
                    self.end_film_id = True
                    logger.info('List of films less than 100')
                    self.state.set_state('%s_end_film_id' % self.table_name, self.end_film_id)

        logger.info('End load')
        return list_film_id

    def loader_raw_data(self) -> [int, list]:
        """Запрос на получение сырых данных"""
        logger.info('Start load')
        list_value = {}

        full_load_sql = sql.SQL('''SELECT DISTINCT fw.id, fw.title, fw.description, fw.rating, fw.type,
                                  fw.updated_at, {array_agg_request}
                                    FROM content.film_work as fw
                                    LEFT JOIN content.person_film_work as pfw ON pfw.film_work_id = fw.id
                                    LEFT JOIN content.person as p ON p.id = pfw.person_id
                                    LEFT JOIN content.genre_film_work as gfw ON gfw.film_work_id = fw.id
                                    LEFT JOIN content.genre as g ON g.id = gfw.genre_id
                                    WHERE fw.id IN %(list_film_id)s
                                    GROUP BY fw.id
                                    ORDER BY fw.updated_at;''')\
            .format(array_agg_request=array_agg_request)
        logger.info('!!!!!! Working with table %s' % self.table_name)
        if self.table_name == 'film_work':
            list_value['list_film_id'] = rows_to_tuple(self.load_list_id())
            self.end_film_id = True
            self.state.set_state('%s_end_film_id' % self.table_name, self.end_film_id)
        else:
            list_value['list_film_id'] = rows_to_tuple(self.load_film_work_id())
        if self.load_list_id_count_zero:
            count = 0
            fetchall = []
            logger.info('Changed data not found')
        else:
            self.cursor.execute(full_load_sql, list_value)
            count = self.cursor.rowcount
            fetchall = self.cursor.fetchall()
        logger.info('End load')
        return count, fetchall
