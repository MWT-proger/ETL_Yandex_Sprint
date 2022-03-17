import json
import time
from functools import wraps


def backoff(logger, start_sleep_time=0.1, factor=2, border_sleep_time=10):

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            count = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    time.sleep(t)
                    if t >= border_sleep_time:
                        t = border_sleep_time
                    if t < border_sleep_time:
                        t *= factor
                    logger.critical(e, exc_info=True)
                    count += 1
                    logger.error(f'Попытка подключение №{count}')
                    continue
                finally:
                    if count == 10:
                        logger.info(
                            f'Выполнено максимальное количество подключений={count}.'
                        )
                        break
        return inner

    return func_wrapper


def rows_to_tuple(rows) -> tuple:
    uuid_list = []
    for row in rows:
        uuid_list.append(row[0])
    return tuple(uuid_list)


def add_movie_in_list(data, obj) -> None:
    data.extend(
        [
            json.dumps(
                {
                    'index': {
                        '_index': 'movies',
                        '_id': obj['id']
                    }
                }
            ),
            json.dumps(obj),
        ]
    )


def work_with_state(logger, state, table_name) -> None:
    end_film_id = state.get_state('%s_end_film_id' % table_name)
    logger.info('end_film_id - %s' % end_film_id)

    if end_film_id:
        state.set_state('%s_last_id_film' % table_name, None)
        state.set_state('%s_end_film_id' % table_name, None)

        temporary_update_at = state.get_state('%s_temporary_update_at' % table_name)
        temporary_id = state.get_state('%s_temporary_id' % table_name)

        state.set_state('%s_last_update_at' % table_name, temporary_update_at)
        state.set_state('%s_last_id' % table_name, temporary_id)
