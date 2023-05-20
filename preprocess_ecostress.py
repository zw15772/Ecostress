# coding=utf-8
import hashlib
import pathlib
import re
from bs4 import BeautifulSoup

from lytools import *
# import mpmath
import requests
import urllib3
from __init__ import *
import urllib
data_root = 'D:\ECOSTRESS\Result\\'


class Download_ECO_Demo:

    def __init__(self):
        self.datadir = T.path_join(data_root, 'Download_ECO_Demo')
        T.mk_dir(self.datadir)
        pass

    def run(self):
        # self.get_xml_url()
        self.download_xml()
        pass

    def get_xml_url(self):
        outdir = join(self.datadir, 'xml_url')
        T.mk_dir(outdir)
        url = 'https://e4ftl01.cr.usgs.gov/ECOSTRESS/ECO2LSTE.001/'
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        for link in tqdm(soup.find_all('a')):
            date = link.get('href')
            if date is not None:
                if date[0] == '2':
                    # fdir_i = join(outdir, date)
                    # T.mk_dir(fdir_i)
                    outf = join(outdir, date[:-1] + '.txt')
                    with open(outf, 'w') as f:
                        url_i = url + date
                        r_i = requests.get(url_i)
                        soup_i = BeautifulSoup(r_i.text, 'html.parser')
                        for link_i in soup_i.find_all('a'):
                            file_name = link_i.get('href')
                            if file_name is not None:
                                if file_name[0] == 'E':
                                    if not file_name.endswith('.xml'):
                                        continue
                                    # out_file = join(fdir_i, file_name)
                                    url_ii = url_i + file_name
                                    f.write(url_ii)
                                    f.write('\n')

    def download_xml(self):
        fdir = join(self.datadir, 'xml_url')
        outdir = join(self.datadir, 'xml')
        T.mk_dir(outdir)
        params_list = []
        for file in tqdm(T.listdir(fdir)):
            file_i = join(fdir, file)
            with open(file_i, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    url = line.strip()
                    # print(line)
                    fname = url.split('/')[-1]
                    date = fname.split('_')[-3]
                    date = date.split('T')[0]
                    outdir_i = join(outdir, date)
                    outf = join(outdir_i, fname)
                    param = (url, outdir_i, outf)
                    params_list.append(param)
        MULTIPROCESS(self.download_i, params_list).run(process=10,process_or_thread='t')

    def download_i(self, params):
        url,outdir_i, outf = params
        try:
            T.mk_dir(outdir_i)
        except:
            pass
        if isfile(outf):
            return
        r_i = requests.get(url)
        with open(outf, 'w') as f:
            f.write(r_i.text)

        pass

    def extract_xml(self):
        fdir= join(self.datadir, 'xml')
        outdir = join(self.datadir, 'xml_extract')
        T.mk_dir(outdir)
        for file in T.list_dir(fdir, 'xml'):
            file_i = join(fdir, file)
            out_file = join(outdir, file)
            with open(file_i, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    if line.find('<Location>') != -1:
                        print(line)
                        exit()
                        with open(out_file, 'w') as f:
                            f.write(line)
                            f.write('\n')
                        break
        pass


def main():
    # ECOSTRESS_download().run()
    Download_ECO_Demo().run()
    pass


if __name__ == '__main__':

    main()