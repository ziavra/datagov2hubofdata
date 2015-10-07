# -*- coding: utf-8 -*-
import json
import os
import datetime
import time
import logging
import pprint
import urllib
from DataGovApi import DataGovApi
import ckanapi
from ckanclient import CkanClient
import cPickle as pickle

__author__ = 'ziavra'


DATAGOV_API_KEY = ''  # API-keys
CKAN_API_KEY = ''

MAINTAINER = 'Vladimir Chaplits'
MAINTAINER_EMAIL = 'vladimir.chaplits@gmail.com'
LICENSE_ID = 'other-open'  # список доступных http://hubofdata.ru/api/3/action/license_list

USE_CKAN_CACHE = True  # кэширует дату модификации для наборов данных на стороне CKAN

class MyPrettyPrinter(pprint.PrettyPrinter):
    def format(self, object, context, maxlevels, level):
        if isinstance(object, unicode):
            return object.encode('utf8'), True, False
        return pprint.PrettyPrinter.format(self, object, context, maxlevels, level)


class DataTransfer(object):
    ckan_url = 'http://hubofdata.ru'
    ckan_perfix = 'data_gov_ru_'  # только нижний регистр
    ckan_perfix_short = 'dgr_'  # только нижний регистр
    dataset_dir = 'datasets/'
    ckan_group = 'data-gov-ru'
    ckan_cache_fname = 'ckan_cache.pkl'
    ckan_cache_ttl = 7  # days

    def __init__(self, ckan_url=''):
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
        self.logger = logging.getLogger('datagov2hubofdata')
        self.logger.info('creating an instance of DataTransfer')
        if not os.path.isdir(self.dataset_dir):
            try:
                os.makedirs(self.dataset_dir)
            except OSError:
                self.logger.error("Failed to open or create directory - %s" % self.dataset_dir)
        try:
            self.dg = DataGovApi(DATAGOV_API_KEY)
        except Exception, e:
            self.dg = None
            self.logger.error("Failed to create an instance of DataGovApi, reason=%s" % e)
            raise
        if not self.dg:
            self.logger.error("Failed to create an instance of DataGovApi")
            exit(1)
        self.ckan = ckanapi.RemoteCKAN(ckan_url if ckan_url else self.ckan_url, CKAN_API_KEY)
        if not self.ckan:
            self.logger.error("Failed to create an instance of CKAN")
            exit(1)
        self.ckan_old = CkanClient(base_location=(ckan_url if ckan_url else self.ckan_url)+'/api', api_key=CKAN_API_KEY)
        if not self.ckan_old:
            self.logger.error("Failed to create an instance of CkanClient(Old API)")
            exit(1)
        self.datasets = []
        self.ckan_cache = {}
        if USE_CKAN_CACHE:  # загружаем кэш
            try:
                self.ckan_cache = pickle.load(open(self.ckan_cache_fname, "rb"))
                self.logger.info("Loaded %d records from cache %s ." % (len(self.ckan_cache), self.ckan_cache_fname))
            except:
                self.ckan_cache = {}
                self.logger.info("Cache is initialized.")

    def get_dataset_list(self, topic='', organization=''):
        """
        Получение списка наборов данных.

        Доступна фильтрация по теме(topic) и по организации(organization)
        :param topic:
        :param organization:
        :return:
        """
        query = {}
        if topic:
            query.update({'topic': topic})
        if organization:
            query.update({'organization': organization})

        data = self.dg.dataset_list('', query)
        if not data:
            self.logger.error("Can not get dataset list.")
            return False
        try:
            self.datasets = json.loads(data)
        except:
            self.logger.error("Can not get dataset list.")
            return False
        self.logger.info("Found %d datasets from API" % len(self.datasets))

    def __pseudo_ckan_create(self, **kwargs):
        res = {"metadata_modified": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')}
        for key, value in kwargs.iteritems():
            # print "%s = " % key,
            # MyPrettyPrinter().pprint(value)
            if key == 'id':
                res.update({'id': value})
        return res

    def process_datasets(self):
        for i in xrange(len(self.datasets)):
            item = self.datasets[i]
            if self.dg.is_duplicated(item['identifier']):
                self.logger.error("Id is duplicated. id=%s" % item['identifier'])
                continue
            # MyPrettyPrinter().pprint(item)
            # if item['identifier'] == '3445126170-go':
            #     del self.datasets[i]

            ckan_package_m_time = datetime.datetime.now()
            ckan_package_id = (self.ckan_perfix_short+item['identifier']).lower()

            data = self.dg.dataset_passport(item['identifier'])
            if data:
                datagov_dataset = json.loads(data)
                # MyPrettyPrinter().pprint(datagov_dataset)
            else:
                self.logger.error("Can't get dataset's passport. id=%s" % item['identifier'])
                continue

            if datagov_dataset["modified"]:
                server_m_time = datetime.datetime.strptime(datagov_dataset["modified"], '%d-%m-%Y')
            elif datagov_dataset["created"]:
                server_m_time = datetime.datetime.strptime(datagov_dataset["created"], '%d-%m-%Y')
            else:
                server_m_time = datetime.datetime.now()

            # набор данных в кэше, кэш не устарел и дата модификации свежее сайта донора
            if USE_CKAN_CACHE and ckan_package_id in self.ckan_cache and \
               self.ckan_cache[ckan_package_id]["checked"] > datetime.datetime.now() - datetime.timedelta(days=self.ckan_cache_ttl) and \
               self.ckan_cache[ckan_package_id]["modified"] >= server_m_time:
                self.logger.info("Package is found in cache and up to date. id=%s" % ckan_package_id)
                continue

            try:
                package_info = self.ckan.action.package_show(id=ckan_package_id)
                ckan_package_m_time = datetime.datetime.strptime(package_info["metadata_modified"], '%Y-%m-%dT%H:%M:%S.%f')
                status = 0
            except ckanapi.NotFound:
                status = 404

            # Если набора данных не существует или дата модификации старше, чем на сайте-доноре
            #print ckan_package_m_time, server_m_time
            if status == 404 or ckan_package_m_time < server_m_time:
                results = {}
                try:  # Проверяем организацию
                    results = self.ckan.action.organization_show(id=self.ckan_perfix+item['organization'])
                    # MyPrettyPrinter().pprint(results)
                    org_status = 0
                except ckanapi.NotFound:
                    org_status = 404
                    results["id"] = ''
                data = self.dg.organization_details(item['organization'])
                # print data
                if data:
                    datagov_org = json.loads(data)
                else:
                    self.logger.error("Can't get organization's details. id=%s, title=%s"%(item['organization'], item['organization_name']))
                    continue

                if org_status == 404:  # Организации не существует
                    try:
                        results = self.ckan.action.organization_create(id=self.ckan_perfix + item['organization'],
                                                                       name=(self.ckan_perfix_short + datagov_org["url"].split('/')[-1]).lower(),
                                                                       title=datagov_org["title"],
                                                                       extras=[{"key": "Ссылка на сайт организации", "value": datagov_org["site-url"]},
                                                                               {"key": "Ссылка на раздел открытых данных", "value": datagov_org["org-site-od-section"]},
                                                                               {"key": "Тип", "value": datagov_org["organization-type"]}])
                    except ckanapi.ValidationError, e:
                        error_str = ", ".join([k+'-'+v[0] for (k, v) in e.error_dict.iteritems() if k != '__type'])
                        self.logger.error("Failed to create organization id=%s error type=Validation Error, reason=%s" % (datagov_org["id"], error_str))
                    except ckanapi.CKANAPIError, e:
                        print e
                        results["id"] = ''

                    self.logger.info("Added organization id=%s, title=%s" % (item['organization'], item['organization_name']))

                # elif status == 0: # Обновляем
                #     try:
                #         results = self.ckan.action.organization_patch(id=self.ckan_perfix+item['organization'],
                #                                                        name=self.ckan_perfix_short + datagov_org["url"].split('/')[-1],
                #                                                        title=datagov_org["title"],
                #                                                        state='active',
                #                                                        extras=[{"key": "site-url", "value": datagov_org["site-url"]},
                #                                                                {"key": "org-site-od-section", "value": datagov_org["org-site-od-section"]},
                #                                                                {"key": "organization-type", "value": datagov_org["organization-type"]}])
                #     except ckanapi.CKANAPIError, e:
                #         print e
                if results["id"] != self.ckan_perfix + item['organization']:
                    self.logger.error("Can't find or create organization id=%s, title=%s" % (item['organization'], item['organization_name']))
                    continue

                # готовим файл
                if datagov_dataset["format"]:
                    ds_format = datagov_dataset["format"]
                elif datagov_dataset["source_url"]:
                    ds_format = datagov_dataset["source_url"].split('.')[-1]
                else:
                    ds_format = ''

                file_ext = '.'+ds_format if ds_format else '.tmp'

                local_fname = (self.dataset_dir+datagov_dataset["id"]+file_ext).lower()
                remote_fname = 'http://data.gov.ru/' + datagov_dataset["source_url"] if datagov_dataset["source_url"][:9] == 'opendata/' else datagov_dataset["source_url"]
                if not remote_fname:
                    self.logger.error("Missing data URL, package id=%s" % ckan_package_id)
                    continue

                if os.path.isfile(local_fname):
                    local_m_time = datetime.datetime.fromtimestamp(os.path.getmtime(local_fname))
                    if local_m_time < server_m_time:
                        try:
                            os.remove(local_fname)
                        except OSError:
                            self.logger.error("Failed to remove file - %s, package id=%s" % (local_fname, ckan_package_id))
                if not os.path.isfile(local_fname):
                    try:
                        urllib.urlretrieve(remote_fname, local_fname)
                    except IOError, e:
                        self.logger.error("Failed to download file %s, reason=%s" % (remote_fname, e))

                if not os.path.isfile(local_fname):
                    self.logger.error("Failed to prepare file %s. Skipping, package id=%s" % (local_fname, ckan_package_id))
                    continue

                tags = [{'name': x.replace('/', '_')} for x in datagov_dataset["subject"].split(', ')] if datagov_dataset["subject"] else []
                tags.append({'name': datagov_dataset["topic"]})
                tags.append({'name': u"data.gov.ru"})
                # MyPrettyPrinter().pprint(tags)
                extras = [{"key": "last_modification_desc", "value": datagov_dataset["last_modification"]},
                          {"key": "organization-type", "value": datagov_dataset["organization-type"]},
                          {"key": "publisher_phone", "value": datagov_dataset["publisher_phone"]}]

                ckan_fname, ckan_error = self.ckan_old.upload_file(local_fname)
                if ckan_error:
                    self.logger.error("Failed to upload file - %s, reason=%s" % (local_fname, ckan_error))
                resources = [{'url': remote_fname.lower(),
                              'description': u'Ссылка на оригинальный набор данных',
                              'format': ds_format,
                              'name': datagov_dataset["title"]},
                             {'url': 'http://data.gov.ru/opendata/'+datagov_dataset["id"].lower(),
                              'description': u'Страница на сайте data.gov.ru',
                              'name': datagov_dataset["title"]},
                             {'url': ckan_fname,
                              'description': u"Данные в формате "+ds_format,
                              'format': ds_format,
                              'resource_type': 'file.upload',
                              'name': datagov_dataset["title"]}]

            if status == 0 and ckan_package_m_time < server_m_time:  # Набор данных существует, обновляем
                package_info["name"] = ckan_package_id
                package_info["title"] = datagov_dataset["title"]
                package_info["author"] = datagov_dataset["publisher"]
                package_info["author_email"] = datagov_dataset["publisher_email"]
                package_info["maintainer"] = MAINTAINER
                package_info["maintainer_email"] = MAINTAINER_EMAIL
                package_info["license_id"] = LICENSE_ID
                package_info["notes"] = datagov_dataset["description"]
                package_info["url"] = 'http://data.gov.ru/opendata/'+datagov_dataset["id"].lower()
                package_info["resources"] = resources
                package_info["tags"] = tags
                package_info["extras"] = extras
                package_info["groups"] = [{"name": self.ckan_group}]
                package_info["owner_org"] = self.ckan_perfix + item['organization']
                try:
                    results1 = self.ckan.action.package_update(**package_info)
                    # MyPrettyPrinter().pprint(results)
                except ckanapi.ValidationError, e:
                    error_str = ", ".join([k+'-'+v[0] for (k, v) in e.error_dict.iteritems() if k != '__type'])
                    self.logger.error("Failed to update package id=%s error type=Validation Error, reason=%s" % (ckan_package_id, error_str))
                except ckanapi.CKANAPIError, e:
                    print e
                self.logger.info("Updated package id=%s title=%s" % (ckan_package_id, datagov_dataset["title"]))
            elif status == 404:  # Добавляем набор данных
                try:
                    results1 = self.ckan.action.package_create(name=ckan_package_id,
                                                              title=datagov_dataset["title"],
                                                              author=datagov_dataset["publisher"],
                                                              author_email=datagov_dataset["publisher_email"],
                                                              maintainer=MAINTAINER,
                                                              maintainer_email=MAINTAINER_EMAIL,
                                                              license_id=LICENSE_ID,
                                                              notes=datagov_dataset["description"],
                                                              url='http://data.gov.ru/opendata/'+datagov_dataset["id"].lower(),
                                                              resources=resources,
                                                              tags=tags,
                                                              extras=extras,
                                                              groups=[{"name": self.ckan_group}],
                                                              owner_org=self.ckan_perfix + item['organization'])
                    # MyPrettyPrinter().pprint(results1)
                except ckanapi.ValidationError, e:
                    error_str = ", ".join([k+'-'+v[0] for (k, v) in e.error_dict.iteritems() if k != '__type'])
                    self.logger.error("Failed to create package id=%s error type=Validation Error, reason=%s" % (ckan_package_id, error_str))
                    continue
                except ckanapi.CKANAPIError, e:
                    print e
                    continue
                self.logger.info("Added package id=%s title=%s" % (ckan_package_id, datagov_dataset["title"]))
            else:
                self.logger.info("Package is up to date. id=%s " % ckan_package_id)

            if USE_CKAN_CACHE:
                if 'results1' in locals():
                    ckan_package_m_time = datetime.datetime.strptime(results1["metadata_modified"], '%Y-%m-%dT%H:%M:%S.%f')
                elif 'package_info' in locals():
                    ckan_package_m_time = datetime.datetime.strptime(package_info["metadata_modified"], '%Y-%m-%dT%H:%M:%S.%f')
                else:
                    continue
                self.ckan_cache[ckan_package_id] = {'modified': ckan_package_m_time, 'checked': datetime.datetime.now()}
                with open(self.ckan_cache_fname, 'wb') as f:
                    try:
                        pickle.dump(self.ckan_cache, f, 2)
                    except pickle.PicklingError:
                        self.logger.error("Failed to save ckan cache %s" % self.ckan_cache_fname)

            # exit()  # TODO убрать после тестов
            time.sleep(5)

    def purge_group_organization(self, id):
        print self.ckan_perfix+id
        try:
            results = self.ckan.action.organization_purge(id=self.ckan_perfix+id)
            results = self.ckan.action.group_purge(id=self.ckan_perfix+id)
        except ckanapi.NotAuthorized, e:
            pass

if __name__ == '__main__':
    try:
        dt = DataTransfer()
    except:
        exit(1)
    dt.get_dataset_list(topic='Weather')
    #dt.purge_group_organization('3444051965')
    dt.process_datasets()
