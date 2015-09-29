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


class MyPrettyPrinter(pprint.PrettyPrinter):
    def format(self, object, context, maxlevels, level):
        if isinstance(object, unicode):
            return object.encode('utf8'), True, False
        return pprint.PrettyPrinter.format(self, object, context, maxlevels, level)


class DataTransfer(object):
    ckan_url = 'http://hubofdata.ru'
    ckan_perfix = 'datagov_'

    def __init__(self, ckan_url=''):
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
        self.logger = logging.getLogger('datagov2hubofdata')
        self.logger.info('creating an instance of DataTransfer')
        self.dg = DataGovApi(DATAGOV_API_KEY)
        if not self.dg:
            self.logger.error("Failed to create an instance of DataGovApi")
            exit(1)
        self.ckan = ckanapi.RemoteCKAN(ckan_url if ckan_url else self.ckan_url)
        if not self.ckan:
            self.logger.error("Failed to create an instance of CKAN")
            exit(1)
        self.datasets = []
        self.registry = {}

    def get_dataset_list(self, load_registry=False, topic='', organization=''):
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
        if load_registry:
            # DEBUG
            cachefname = 'registry.csv'
            if os.path.exists(cachefname) and (time.time()-os.stat(cachefname).st_mtime) < 60*30*3:  # 90 mins
                with open(cachefname, 'r') as f:
                    data = f.read()
            else:
                data = self.dg.registry()
                with open(cachefname, 'w+') as f:
                    f.write(data)
            if not data:
                self.logger.error("Can not get registry.")
                return False

            f = StringIO.StringIO(data)
            for row in list(UnicodeReader(f, delimiter=';', quotechar='"')):
                if row[0] == u"Название набора":
                    continue
                self.registry[row[1]] = row[0:1] + row[2:]
            self.logger.info("Found %d datasets from registry" % len(self.registry))

    def process_datasets(self):
        for i in xrange(len(self.datasets)-1):
            item = self.datasets[i]
            MyPrettyPrinter().pprint(item)
            if self.registry:
                MyPrettyPrinter().pprint(self.registry[item['identifier']])
            # if item['identifier'] == '3445126170-go':
            #     del self.datasets[i]
            try:
                results = self.ckan.action.package_show(id=self.ckan_perfix+item['identifier'])
                status = 0
            except ckanapi.NotFound:
                status = 404
                results = {'success': False}
            if results['success'] and status != 404:  # Существует
                pass
            else:  # Добавляем
                try:
                    results = self.ckan.action.organization_show(id=self.ckan_perfix+item['organization'])
                    status = 0
                except ckanapi.NotFound:
                    status = 404
                    results = {'success': False}
                if not results['success'] or status == 404:  # Не существует
                    # dataGovOrg = self.dg.organization(item['organization'])
                    # results = self.ckan.action.organization_create(id=self.ckan_perfix+item['organization'])
                    pass
            # results= self.ckan.action.package_search(q='"'+item['title']+'"')
            # if results['count']>0:
            #     for  res in results['results']:
            #         MyPrettyPrinter().pprint(res)

if __name__ == '__main__':
    dt = DataTransfer()
    dt.get_dataset_list(True, topic='cartography')
    dt.process_datasets()

#    dg = DataGovApi(DATAGOV_API_KEY)
#    data = dg.dataset_list()

#    print dg.dataset('2983997167-gosudarstvennyj-kontrol-nadzor')
#    print dg.dataset_version_list('2983997167-gosudarstvennyj-kontrol-nadzor')
#    print dg.dataset_version('2983997167-gosudarstvennyj-kontrol-nadzor', '20140711T135451')
#    print dg.organization('7107027505')
