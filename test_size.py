from csvUnicode import UnicodeReader
import requests

__author__ = 'ziavra'

if __name__ == '__main__':
    registryfname='registry.csv'
    with open(registryfname, 'r') as f:
        registry = list(UnicodeReader(f, delimiter=';', quotechar='"'))
    del registry[0]
    print "Loaded %d records" % len(registry)

    for row in registry:
        url = row[15]
        if url[:9] == 'opendata/':
            url = 'http://data.gov.ru/' + url
        resp = requests.head(url)
        print resp.status_code, resp.text, resp.headers
        exit()