# -*- coding: utf-8 -*-
import urllib2
import urllib
import httplib
import logging
import re
import json
import os
import time
import StringIO
from csvUnicode import UnicodeReader

__author__ = 'ziavra'


class DataGovApi(object):
    api_domain = 'data.gov.ru'
    base_url = 'http://'+api_domain+'/api'
    __registry_cachefname = 'registry.csv'

    def __init__(self, access_token, _format='json'):
        """

        :param str access_token:
        :param str _format: 'json' | 'xml'
        """
        # logging.info('test info')
        logging.getLogger(__name__).addHandler(logging.NullHandler())
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        if not access_token:
            self.logger.error('Missed access_token')
        self.logger.info('creating an instance of DataGovApi')
        self.access_token = access_token
        self.format = _format
        self.__registry = {}

    def __get_web_page(self, url, ref='', query=None):
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0'
        headers = {'User-Agent': user_agent, 'Referer': ref}

        data = urllib.urlencode(query, True) if query else None

        req = urllib2.Request(url, data, headers)
        html = ''
        try:
            response = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            self.logger.error('HTTPError = ' + str(e.code))
        except urllib2.URLError, e:
            self.logger.error('URLError = ' + str(e.reason))
        except httplib.HTTPException, e:
            self.logger.error(e)
        except Exception:
            import traceback
            self.logger.error('generic exception: ' + traceback.format_exc())
            raise
        else:
            html = response.read()

        return html

    def __get_api_page(self, url, _format='', query=None):
        url = self.base_url + '/' + (_format if _format else self.format) + ('/' + url if url else '')
        if query:
            query.update({'access_token': self.access_token})
        else:
            query = {'access_token': self.access_token}
        query = urllib.urlencode(query, True)
        self.logger.debug(url + '?' + query)
        data = self.__get_web_page(url + '?' + query)
        return unicode(data, 'utf-8')

    def main_page(self, _format=''):
        """
        Запрос предназначен для предоставления информации об API, в том числе справочной информации, условиях использования и перечня возможных запросов.

        :param _format:
        :return:
        """
        data = self.__get_api_page('', _format)
        return data

    def dataset_list(self, _format='', query=None):
        """
        Запрос предназначен для получения перечня наборов открытых данных.
        :param _format:
        :param query:
        :return:
        """
        data = self.__get_api_page('dataset', _format, query)
        return data

    def dataset(self, ds_id, _format=''):
        """
        Запрос предназначен для получения набора открытых данных.
        :param ds_id:
        :param _format:
        :return:
        """
        if not ds_id:
            return False
        data = self.__get_api_page('dataset/%s' % ds_id, _format)
        return data

    def dataset_version_list(self, ds_id, _format=''):
        """
        Запрос предназначен для получения перечня версий набора открытых данных.
        :param ds_id:
        :param _format:
        :return:
        """
        if not ds_id:
            return False
        data = self.__get_api_page('dataset/%s/version' % ds_id, _format)
        return data

    def dataset_version(self, ds_id, version_id, _format=''):
        """
        Запрос предназначен для получения полной информации о версии набора открытых данных.

        :param ds_id:
        :param version_id:
        :param _format:
        :return:
        """
        if not ds_id or not version_id:
            return False
        data = self.__get_api_page('dataset/%s/version/%s' % (ds_id, version_id), _format)
        return data

    def dataset_version_structure(self, ds_id, version_id, _format=''):
        """
        Запрос предназначен для получения полной информации о структуре версии набора открытых данных.

        :param ds_id:
        :param version_id:
        :param _format:
        :return:
        """
        if not ds_id or not version_id:
            return False
        data = self.__get_api_page('dataset/%s/version/%s/structure' % (ds_id, version_id), _format)
        return data

    def dataset_version_content(self, ds_id, version_id, _format='', search=''):
        """
        Запрос предназначен для получения содержимого файла версии набора открытых данных.

        :param ds_id:
        :param version_id:
        :param _format:
        :param search:
        :return:
        """
        if not ds_id or not version_id:
            return False
        query = {'search': search} if search else None
        data = self.__get_api_page('dataset/%s/version/%s/content' % (ds_id, version_id), _format, query)
        return data

    def organization_list(self, _format=''):
        """
        Запрос предназначен для получения перечня зарегистрированных на портале организаций,
        предоставляющих открытые данные.
        :param _format:
        :return:
        """
        data = self.__get_api_page('organization', _format)
        return data

    def organization(self, org_id, _format=''):
        """
        Запрос предназначен для получения полных данных организации, предоставляющей открытые данные.

        :param org_id:
        :param _format:
        :return:
        """
        if not org_id:
            return False
        data = self.__get_api_page('organization/%s' % org_id, _format)
        return data

    def dataset_list_by_organization(self, org_id, _format='', topic=''):
        """
        Запрос предназначен для получения перечня наборов открытых данных.

        :param org_id:
        :param _format:
        :param topic:
        :return:
        """
        if not org_id:
            return False
        query = {'topic': topic} if topic else None
        data = self.__get_api_page('organization/%s/dataset' % org_id, _format, query)
        return data

    def topic_list(self, _format=''):
        """
        Запрос предназначен для получения перечня тематических рубрик наборов открытых данных.

        :param _format:
        :return:
        """
        data = self.__get_api_page('topic', _format)
        return data

    def topic(self, topic_id, _format=''):
        """
        Запрос предназначен для получения тематической рубрики.

        :param topic_id:
        :param _format:
        :return:
        """
        if not topic_id:
            return False
        data = self.__get_api_page('topic/%s' % topic_id, _format)
        return data

    def dataset_list_by_topic(self, topic_id, _format='', org_id=''):
        """
        Запрос предназначен для получения перечня наборов открытых данных по тематической рубрике.

        :param topic_id:
        :param _format:
        :param org_id:
        :return:
        """
        if not topic_id:
            return False
        query = {'organization': org_id} if org_id else None
        data = self.__get_api_page('topic/%s/dataset' % topic_id, _format, query)
        return data

    def registry(self):
        """
        Возвращает реестр наборов открытых данных в формате csv. Не входит в официальное API.
        :rtype : string
        :return:
        """

        if os.path.exists(self.__registry_cachefname) and (time.time()-os.stat(self.__registry_cachefname).st_mtime) < 60*30*3:  # 90 mins
            with open(self.__registry_cachefname, 'r') as f:
                data = f.read()
        else:
            data = self.__get_web_page('http://'+self.api_domain+'/opendata/export/csv', 'http://'+self.api_domain+'/opendata')
            with open(self.__registry_cachefname, 'w+') as f:
                f.write(data)
        if not data:
            self.logger.error("Can not get registry.")
            return False

        f = StringIO.StringIO(data)
        for row in list(UnicodeReader(f, delimiter=';', quotechar='"')):
            if row[0] == u"Название набора":
                continue
            self.__registry[row[1]] = row[0:1] + row[2:]

        return data

    def organization_details(self, inn='', _format=''):
        """
        Возвращает данные организации. Не входит в официальное API.

        :rtype : string
        :param str inn:
        :param _format:
        """
        if not inn:
            return False

        base_url = 'http://'+self.api_domain
        query = {'field_organization_inn_value': inn,
                 'view_name': 'organizations',
                 'view_display_id': 'organizations_list'}
        result = self.__get_web_page(base_url+'/views/ajax', base_url+'/organizations', query)
        result = unicode(result, 'unicode-escape')
        result = result.replace('\/', '/')

        p = re.compile(u'\s+<tr class="odd views-row-first views-row-last">\n'
                       u'\s+<td class="views-field views-field-counter" >\n'
                       u'\s+1          </td>\n'
                       u'\s+<td class="views-field views-field-title" >\n'
                       u'\s+<a href="(/.*)">(.*)</a>          </td>\n'
                       u'\s+<td class="views-field views-field-field-organization-type views-align-center" >\n'
                       u'\s+(.*)</td>\n'
                       u'\s+<td class="views-field views-field-field-organization-inn views-align-center" >\n'
                       u'\s+(\d+)          </td>\n'
                       u'\s+<td class="views-field views-field-field-site-url" >\n'
                       u'\s+<a href="(.*)" rel="nofollow" target="_blank">Сайт организации</a>          </td>\n'
                       u'\s+<td class="views-field views-field-field-org-site-od-section" >\n'
                       u'\s+<a href="(.*)">Ссылка на раздел открытых данных</a>          </td>\n'
                       u'\s+<td class="views-field views-field-created views-align-center" >\n'
                       u'\s+(.*)          </td>\n'
                       u'\s+<td class="views-field views-field-nid views-align-center" >\n'
                       u'\s+(\d+)          </td>\n'
                       u'\s+</tr>')

        matches = re.search(p, result)
        if not matches:
            return False

        fmt_result = None
        tmp_format = _format if _format else self.format
        if tmp_format == 'json':
            json_result = {"url": base_url+matches.group(1),
                           "title": matches.group(2),
                           "organization-type": matches.group(3),
                           "id": matches.group(4),
                           "site-url": matches.group(5),
                           "org-site-od-section": matches.group(6),
                           "created": matches.group(7),
                           "dataset-count": int(matches.group(8))}
            fmt_result = json.dumps(json_result, indent=4)
        elif tmp_format == 'xml':
            # TODO добавить генерацию XML
            pass
        else:
            return False

        return fmt_result

    def dataset_passport(self, ds_id, _format=''):
        """
        Возвращает паспорт набора данных, работает через реестр. Не входит в официальное API.

        :param ds_id:
        :param _format:
        :return: string
        """
        if not ds_id:
            return False
        # TODO добавить получение информации со страницы набора
        if not self.__registry:
            self.registry()

        data = self.__registry[ds_id]
        if not data:
            return False

        fmt_result = None
        tmp_format = _format if _format else self.format
        if tmp_format == 'json':
            json_result = {"id": ds_id,
                           "title": data[0],
                           "description": data[1],
                           "creator": data[2],
                           "publisher": data[3],
                           "publisher_phone": data[4],
                           "publisher_email": data[5],
                           "format": data[6],
                           "valid": data[7],
                           "created": data[8],
                           "modified": data[9],
                           "last_modification": data[10],
                           "topic": data[11],
                           "organization-type": data[12],
                           "subject": data[13],
                           "source_url": data[14],
                           "structure_url": data[15]}
            fmt_result = json.dumps(json_result, indent=4)
        elif tmp_format == 'xml':
            # TODO добавить генерацию XML
            pass
        else:
            return False

        return fmt_result


if __name__ == "__main__":
    dg = DataGovApi('***REMOVED***')
    # print dg.main_page()
    # data=dg.dataset_list('', {"topic": "Government"})
    # json_data = json.loads(data)
    # for item in json_data:
    #     print item['identifier'], item['title'], item['organization'], item['organization_name'], item['topic']
    # print dg.dataset('7710349494-mfclist')
    # print dg.dataset_version_list('7710349494-mfclist')
    # print dg.dataset_version('7710349494-mfclist', '20131201T134500')
    # print dg.dataset_version_structure('7710349494-mfclist', '20131201T134500')
    # print dg.dataset_version_content('7710349494-mfclist', '20131201T134500', search='Клейменычев')
    # print dg.organization_list()
    # print dg.organization('7705884873')
    # print dg.dataset_list_by_organization('7705884873', topic="Government")
    # print dg.topic_list()
    # print dg.topic("Government")
    # print dg.dataset_list_by_topic("Government")
    # dg.registry()
    # print dg.organization_details('3444051965')
    # print dg.dataset_passport('7710349494-mfclist')
    pass
