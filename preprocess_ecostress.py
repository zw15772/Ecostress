# coding=utf-8
import hashlib
import linecache
import pathlib
import re
from bs4 import BeautifulSoup

from lytools import *
# import mpmath
import requests
import urllib3
from __init__ import *
import urllib
import shp_process as sb


data_root = 'D:\ECOSTRESS\Result\\'
shpfile= 'D:\ECOSTRESS\Data\western US shp\western_US_simplify\\western_US_simplify.shp'


class Download_ECO:

    def __init__(self):
        self.datadir = T.path_join(data_root, 'Download_ECO')
        T.mk_dir(self.datadir)
        pass

    def run(self):
        # self.get_xml_url()
        # self.download_xml()
        self.extract_xml()
        self.extract_western_US()
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
        result_all_dict = {}
        list_name = []
        list_spatial_upperleft = []
        list_spatial_upperright = []
        list_spatial_lowerleft = []
        list_spatial_lowerright = []
        list_size = []
        list_time_start = []

  ##########start to extract xml file
        fdir= join(self.datadir, 'xml')
        outdir = join(self.datadir, 'xml_extract')
        outf = join(outdir, 'extraction_result.df')
        T.mk_dir(outdir)


        for fdir_sub in tqdm(T.listdir(fdir)):
            fdir_sub_i = join(fdir, fdir_sub)

            for file in T.listdir(fdir_sub_i):
                file_i = join(fdir_sub_i, file)

                with open(file_i, 'r') as f:
                    content= f.read()
                    soup = BeautifulSoup(content, 'html.parser')
                    # print(soup)
                    # exit()

                    for line in soup.find_all('distributedfilename'):
                        name = line.get_text()
                        list_name.append(name)
                    for line in soup.find_all('westboundingcoordinate'):
                        upperleftx = line.get_text()
                        float_upperleftx = float(upperleftx)

                        lowerleftx= line.get_text()
                        float_lowerleftx = float(lowerleftx)

                    for line in soup.find_all('eastboundingcoordinate'):
                        upperrightx = line.get_text()
                        floatupperrightx = float(upperrightx)
                        lowerrightx = line.get_text()
                        floatlowerrightx = float(lowerrightx)
                    for line in soup.find_all('northboundingcoordinate'):
                        upperlefty = line.get_text()
                        float_upperlefty = float(upperlefty)
                        upperrighty = line.get_text()
                        float_upperrighty = float(upperrighty)
                    for line in soup.find_all('southboundingcoordinate'):
                        lowerlefty = line.get_text()
                        float_lowerlefty = float(lowerlefty)
                        lowerrighty = line.get_text()
                        float_lowerrighty = float(lowerrighty)


                    list_spatial_upperleft.append([float_upperleftx, float_upperlefty])
                    list_spatial_upperright.append([floatupperrightx, float_upperrighty])
                    list_spatial_lowerleft.append([float_lowerleftx, float_lowerlefty])
                    list_spatial_lowerright.append([floatlowerrightx, float_lowerrighty])
            #///////////////////////extract size
                    for line in soup.find_all('filesize'):
                        size = line.get_text()
                        size_int= size
                        list_size.append(size_int)
            #///////////////////////extract time
                    for line in soup.find_all('distributedfilename'):
                        temp = line.get_text()
                        time= temp.split('_')[5].split('T')[0]+'_'+temp.split('_')[5].split('T')[1].split('.')[0][0:4]
                        # print(time)

                        list_time_start.append(time)


        result_all_dict['id']=list(range(1,len(list_name)+1))
        result_all_dict['file_name']=list_name
        result_all_dict['spatial_upperleft']=list_spatial_upperleft
        result_all_dict['spatial_upperright']=list_spatial_upperright
        result_all_dict['spatial_lowerleft']=list_spatial_lowerleft
        result_all_dict['spatial_lowerright']=list_spatial_lowerright
        result_all_dict['size']=list_size
        result_all_dict['time_start']=list_time_start
        df = pd.DataFrame(result_all_dict)
        T.print_head_n(df)
        T.save_df(df, outf)
        T.df_to_excel(df,outf.replace('.df',''))

#///////////////////////extract western US images from shapefile
    def extract_western_US (self):

        fdir = join(self.datadir, 'xml_extract')
        f= join(fdir, 'extraction_result.df')
        outf= join(fdir, 'extraction_result_western_US.df')

        df = T.load_df(f)
        str_list=[]
        all_list=[]
        upperleft_x_y_list=df['spatial_upperleft']=df['spatial_upperleft'].tolist()
        upperright_x_y_list=df['spatial_upperright']=df['spatial_upperright'].tolist()
        lowerleft_x_y_list=df['spatial_lowerleft']=df['spatial_lowerleft'].tolist()
        lowerright_x_y_list=df['spatial_lowerright']=df['spatial_lowerright'].tolist()
        all_list=[upperleft_x_y_list,upperright_x_y_list,lowerleft_x_y_list,lowerright_x_y_list]
        check_all = []
        is_in_shapefile=[]
        for i in range(len(all_list)):
            all_list_i=all_list[i]

            x_list=[]
            y_list=[]
            check=[]

            for i in range(len(all_list_i)):
                x_y_list_i=all_list_i[i]
                x=x_y_list_i[0]
                y=x_y_list_i[1]
                x_list.append(x)
                y_list.append(y)
                check=(self.check(x_list,y_list))
                check_all.append(check)
        array_check=np.array(check_all)
        array_check=array_check.reshape(len(all_list_i),4)
        for i in range(len(array_check)):

            if False in array_check[i]:
                is_in_shapefile.append('False')
            else:
                is_in_shapefile.append('True')

        df['is_in_shapefile']=is_in_shapefile
        T.print_head_n(df)
        T.save_df(df, outf)
        T.df_to_excel(df, outf.replace('.df', ''))

    def check(self, x_list, y_list): # check if images are in the shapefile range
        poly=sb.get_poly(shpfile)
        check={}
        flag=0
        flag1=0
        for i in range(len(x_list)):
            flag+=1
            if flag % (len(x_list)/10) ==0:
                flag1+=10
            x=float(x_list[i])
            y=float(y_list[i])

            check=sb.check_xys_in_poly(x, y, poly, )
        return check

    def statistic_anaysis_data_availibility(self):
        pass

def main():
    # ECOSTRESS_download().run()
    Download_ECO().run()
    pass


if __name__ == '__main__':

    main()