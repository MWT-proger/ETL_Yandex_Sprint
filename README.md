# Проектное задание: ETL

## Задание спринта

Написать отказоустойчивый перенос данных из Postgres в Elasticsearch

## Требования

- Используйте предложенную [cхему индекса](https://code.s3.yandex.net/middle-python/learning-materials/es_schema.txt)💾  `movies`, в которую должна производиться загрузка фильмов. В конце этого урока вы найдёте пояснения к ней.
- Ваш код должен корректно вести себя при потере связи с ES или Postgres. Используйте технику backoff, чтобы ваш сервис не мешал восстановлению БД.
- При перезапуске приложения оно должно продолжить работу с места остановки, а не начинать процесс заново. Здесь вам поможет хранение состояния.
- ES с загруженными данными успешно проходит [Postman-тесты](https://code.s3.yandex.net/middle-python/learning-materials/ETLTests-2.json)💾. Подробнее об этом способе тестирования мы расскажем в следующем уроке.

**Если придумали свой вариант архитектуры, то необходимо вначале показать её наставнику.**

И ещё несколько советов:

- валидируйте конфигурации с помощью `pydantic`;
- избегайте дублирования кода и SQL-запросов;
- используйте аннотации типов;
- документируйте функции, используя комментарии;
- для логирования используйте модуль `logging` из стандартной библиотеки Python;
- соблюдайте `PEP8`.

## Вам пригодится

- освежить в памяти базовые элементы SQL-запросов: `SELECT` и `JOIN`;
- локально поднять сервер Elasticsearch, используя Docker;
- определиться с архитектурой скрипта. Очень помогает нарисовать её на листочке и разбить на основные элементы. Подсказка: такими элементами могут быть загрузка данных из Postgres, преобразование каждой строки фильма в удобный для загрузки формат и подготовка данных для загрузки в Elasticsearch.

Желаем вам удачи в написании ETL! Вы обязательно справитесь 💪 

## Объяснение схемы данных индекса movies

Это задание будет достаточно сложно выполнить, не имея никакого представления о схеме индекса в Elasticsearch.

Пройдёмся по основным элементам схемы данных:

- `"refresh_interval": "1s"` — при сохранении данных обновляет индекс раз в секунду.
- Блок `"analysis"` — в нём задаются все настройки для полнотекстового поиска: фильтры и анализаторы. Незаменимая вещь для задачи поиска по тексту.
- В каждой схеме данных указано `"dynamic": "strict"` — это позволяет защититься от невалидных данных.
- Поля `actors` и `writers` используют вложенную схему данных — это помогает валидировать вложенные json-объекты.
- Также присутствуют поля `actors_names` и `writers_names`, которые упрощают запросы на поиск. Вам это понадобится в следующих модулях.
- Поле `title` содержит внутри себя ещё одно поле — `title.raw`. Оно нужно, чтобы у Elasticsearch была возможность делать сортировку, так как он не умеет сортировать данные по типу `text`. 



Возможны и другие оптимизации, но для текущей задачи этих настроек будет достаточно.

