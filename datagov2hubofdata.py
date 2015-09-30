# -*- coding: utf-8 -*-
import json, StringIO
import os, time
import logging
import pprint
from DataGovApi import DataGovApi
from csvUnicode import UnicodeReader
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

    def __init__(self, ckan_url=''):
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
        self.logger = logging.getLogger('datagov2hubofdata')
        self.logger.info('creating an instance of DataTransfer')
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
            else:  # Добавляем
                try:
                    results = self.ckan.action.organization_show(id=self.ckan_perfix+item['organization'])
                    # MyPrettyPrinter().pprint(results)
                    status = 0
                except ckanapi.NotFound:
                    status = 404
                data = self.dg.organization_details(item['organization'])
                print data
                if data:
                    dataGovOrg = json.loads(data)
                if status == 404:  # Не существует
                    try:
                        results = self.ckan.action.organization_create(id=self.ckan_perfix+item['organization'],
                                                                       name=dataGovOrg["url"].split('/')[-1],
                                                                       title=dataGovOrg["title"],
                                                                       extras=[{"key": "site-url", "value": dataGovOrg["site-url"]},
                                                                               {"key": "org-site-od-section", "value": dataGovOrg["org-site-od-section"]},
                                                                               {"key": "organization-type", "value": dataGovOrg["organization-type"]}])
                    except ckanapi.CKANAPIError, e:
                        print e
                # elif status == 0:
                #     try:
                #         results = self.ckan.action.organization_update(id=self.ckan_perfix+item['organization'],
                #                                                        name=dataGovOrg["url"].split('/')[-1],
                #                                                        title=dataGovOrg["title"],
                #                                                        state='active',
                #                                                        extras=[{"key": "site-url", "value": dataGovOrg["site-url"]},
                #                                                                {"key": "org-site-od-section", "value": dataGovOrg["org-site-od-section"]},
                #                                                                {"key": "organization-type", "value": dataGovOrg["organization-type"]}])
                #     except ckanapi.CKANAPIError, e:
                #         print e

                exit()
            # results= self.ckan.action.package_search(q='"'+item['title']+'"')
            # if results['count']>0:
            #     for  res in results['results']:
            #         MyPrettyPrinter().pprint(res)

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
