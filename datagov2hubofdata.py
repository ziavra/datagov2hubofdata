# -*- coding: utf-8 -*-
import json
import os
import datetime
import logging
import pprint
import urllib
from DataGovApi import DataGovApi
import ckanapi

__author__ = 'ziavra'


DATAGOV_API_KEY = '***REMOVED***'
CKAN_API_KEY = '***REMOVED***'


class MyPrettyPrinter(pprint.PrettyPrinter):
    def format(self, object, context, maxlevels, level):
        if isinstance(object, unicode):
            return object.encode('utf8'), True, False
        return pprint.PrettyPrinter.format(self, object, context, maxlevels, level)


class DataTransfer(object):
    ckan_url = 'http://hubofdata.ru'
    ckan_perfix = 'data_gov_ru_'
    ckan_perfix_short = 'dgr_'
    dataset_dir = 'datasets/'

    def __init__(self, ckan_url=''):
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
        self.logger = logging.getLogger('datagov2hubofdata')
        self.logger.info('creating an instance of DataTransfer')
        if not os.path.isdir(self.dataset_dir):
            try:
                os.makedirs(self.dataset_dir)
            except OSError:
                self.logger.error("Failed to open or create directory - %s" % self.dataset_dir)
        self.dg = DataGovApi(DATAGOV_API_KEY)
        if not self.dg:
            self.logger.error("Failed to create an instance of DataGovApi")
            exit(1)
        self.ckan = ckanapi.RemoteCKAN(ckan_url if ckan_url else self.ckan_url, CKAN_API_KEY)
        if not self.ckan:
            self.logger.error("Failed to create an instance of CKAN")
            exit(1)
        self.datasets = []

    def get_dataset_list(self, topic='', organization=''):
        query = {}
        if topic:
            query.update({'topic': topic})
        if organization:
            query.update({'organization': organization})

        data = self.dg.dataset_list('', query)
        if not data:
            self.logger.error("Can not get dataset list.")
            return False
        self.datasets = json.loads(data)
        self.logger.info("Found %d datasets from API" % len(self.datasets))

    def process_datasets(self):
        for i in xrange(len(self.datasets)-1):
            item = self.datasets[i]
            if self.dg.is_duplicated(item['identifier']):
                continue
            MyPrettyPrinter().pprint(item)
            # if item['identifier'] == '3445126170-go':
            #     del self.datasets[i]
            try:
                results = self.ckan.action.package_show(id=self.ckan_perfix+item['identifier'])
                status = 0
            except ckanapi.NotFound:
                status = 404
            if status != 404:  # Существует
                pass
            else:  # Добавляем набор данных
                try:
                    results = self.ckan.action.organization_show(id=self.ckan_perfix+item['organization'])
                    # MyPrettyPrinter().pprint(results)
                    status = 0
                except ckanapi.NotFound:
                    status = 404
                    results["id"] = ''
                data = self.dg.organization_details(item['organization'])
                # print data
                if data:
                    datagov_org = json.loads(data)
                else:
                    self.logger.error("Can't get organization's details. id=%s, title=%s"%(item['organization'], item['organization_name']))
                    return False

                if status == 404:  # Не существует
                    try:
                        results = self.ckan.action.organization_create(id=self.ckan_perfix + item['organization'],
                                                                       name=self.ckan_perfix_short + datagov_org["url"].split('/')[-1],
                                                                       title=datagov_org["title"],
                                                                       extras=[{"key": "site-url", "value": datagov_org["site-url"]},
                                                                               {"key": "org-site-od-section", "value": datagov_org["org-site-od-section"]},
                                                                               {"key": "organization-type", "value": datagov_org["organization-type"]}])
                    except ckanapi.CKANAPIError, e:
                        print e
                        results["id"] = ''
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
                if not results["id"] == self.ckan_perfix + item['organization']:
                    self.logger.error("Can't find or create organization id=%s, title=%s"%(item['organization'], item['organization_name']))
                    return False

                data = self.dg.dataset_passport(item['identifier'])
                print data
                if data:
                    datagov_dataset = json.loads(data)
                else:
                    self.logger.error("Can't get dataset's passport. id=%s" % item['identifier'])
                    return False

                if datagov_dataset["modified"]:
                    server_m_time = datetime.datetime.strptime(datagov_dataset["modified"], '%d-%m-%Y')
                elif datagov_dataset["created"]:
                    server_m_time = datetime.datetime.strptime(datagov_dataset["created"], '%d-%m-%Y')
                else:
                    server_m_time = datetime.datetime.now()

                if datagov_dataset["format"]:
                    file_ext = '.'+datagov_dataset["format"]
                elif datagov_dataset["source_url"]:
                    file_ext = '.'+datagov_org["source_url"].split('.')[-1]
                else:
                    file_ext = '.'+'tmp'

                local_fname = self.dataset_dir+datagov_org["id"]+file_ext
                remote_fname = 'http://data.gov.ru/' + datagov_dataset["source_url"] if datagov_dataset["source_url"][:9] == 'opendata/' else datagov_dataset["source_url"]
                if os.path.isfile(local_fname):
                    local_m_time = datetime.datetime.fromtimestamp(os.path.getmtime(local_fname))
                if not os.path.isfile(local_fname) or local_m_time < server_m_time:
                    urllib.urlretrieve(remote_fname, local_fname)
            exit()

    def purge_group_organization(self, id):
        print self.ckan_perfix+id
        try:
            results = self.ckan.action.organization_purge(id=self.ckan_perfix+id)
            results = self.ckan.action.group_purge(id=self.ckan_perfix+id)
        except ckanapi.NotAuthorized, e:
            pass

if __name__ == '__main__':
    dt = DataTransfer()
    dt.get_dataset_list(topic='cartography')
    #dt.purge_group_organization('3444051965')
    dt.process_datasets()

#    dg = DataGovApi(DATAGOV_API_KEY)
#    data = dg.dataset_list()

#    print dg.dataset('2983997167-gosudarstvennyj-kontrol-nadzor')
#    print dg.dataset_version_list('2983997167-gosudarstvennyj-kontrol-nadzor')
#    print dg.dataset_version('2983997167-gosudarstvennyj-kontrol-nadzor', '20140711T135451')
#    print dg.organization('7107027505')
