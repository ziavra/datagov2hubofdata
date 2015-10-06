# crimestat_parser

Парсер данных из data.gov.ru в hubofdata.ru

Основной файл - datagov2hubofdata.py, DataGovApi.py реализация официального API + несколько функций вне API.
Для работы с CKAN используется [ckanapi](https://github.com/ckan/ckanapi), но файлы в CKAN заливаются с помощью [ckanclient](https://github.com/okfn/ckanclient-deprecated) из-за старой версии(2.2a) ckan на хостинге.
