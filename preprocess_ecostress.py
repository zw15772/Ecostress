# coding=utf-8
import hashlib
import linecache
import pathlib
import re

import matplotlib.pyplot as plt
from bs4 import BeautifulSoup

from lytools import *
# import mpmath
import requests
import urllib3
from __init__ import *
import urllib
import shp_process as sb


data_root = 'D:\ECOSTRESS\Result\\'
# for server
# data_root = '/data/home/wenzhang/ECOSTRESS/'
shpfile= 'D:\ECOSTRESS\Data\western US shp\western_US_simplify\\western_US_simplify.shp'


class Download_ECO:

    def __init__(self):
        self.datadir = T.path_join(data_root, 'Download_ECO')
        T.mk_dir(self.datadir)
        pass

    def run(self):
        # self.get_xml_url()
        # self.download_xml()
        # self.extract_xml()
        # self.delete_error_files()
        # self.extract_western_US()
        # self.filter()
        # self.gen_hdf5_url_list()
        self.data_download()
        # self.statistic_anaysis_temporal()
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

    def download_i(self, params):   # 文本
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

    def download_hdf_i(self, params):
        url, outdir_i, outf = params
        try:
            T.mk_dir(outdir_i)
        except:
            pass
        if isfile(outf):
            return
        r_i = requests.get(url)
        with open(outf, 'wb') as f:
            f.write(r_i.content)
        pass

    def delete_error_files(self):
        fatherdir = join(self.datadir, 'xml')
        for fdir in tqdm(T.listdir(fatherdir)):
            for file in tqdm(T.listdir(join(fatherdir, fdir))):
                file_i = join(fatherdir, fdir, file)
                ######delete error files when size <3kb
                size= os.path.getsize(file_i)

                if size < 3000:
                    os.remove(file_i)
                    print(file_i)

    def filter(self):
        fdir = join(self.datadir, 'xml_extract')
        f = join(fdir, 'extraction_result_western_US_all.df')
        df = T.load_df(f)
        flie_size_list = []
        df_filter = df[df['is_in_shapefile'] == 'True']
        print('number of images in shapefile is: ', len(df_filter))

        # ///////////////////////statistic analysis diural
        time_start_list = df['time_start']
        date_list = []
        diural_mark_list = []
        year_month_list = []
        morning_list = list(range(0, 12))
        afternoon_list = list(range(12, 24))
        for i in time_start_list:
            year_month = i.split('_')[0][0:6]
            year_month_list.append(year_month)
            date = i.split('_')[0]
            date_list.append(date)
            time_hour = i.split('_')[1][0:2]
            if int(time_hour) in morning_list:
                diural_mark_list.append('morning')
            elif int(time_hour) in afternoon_list:
                diural_mark_list.append('afternoon')
            else:
                print('error')
                print(time_hour)
                exit()

        df['date'] = date_list
        df['diural'] = diural_mark_list
        df['year_month'] = year_month_list
        df_filter = df[df['is_in_shapefile'] == 'True']
        ##### print afternoon ration
        # afternoon_df=df_filter[df_filter['diural']=='afternoon']
        # afternoon_df_size=len(afternoon_df)/len(df_filter)
        # print('afternoon size is: ',afternoon_df_size)
        month_list = list(range(6, 11))
        year_list = list(range(2020, 2022))
        date_selection_list = []

        for i in year_list:
            int_year = int(i)
            for j in month_list:
                j = ('%.2d' % j)
                date = str(int_year) + str(j)
                date_selection_list.append(date)

        df_filter_date = df_filter[df_filter['year_month'].isin(date_selection_list)]
        print('number of images in shapefile and date selection is: ', len(df_filter_date))

        # size_list = df_filter_date['size']
        # for i in size_list:
        #     flie_size_list.append(int(i))
        # print('sum size is: ', sum(flie_size_list)/1024/1024/1024/1024)
        url_list=[]
        for i in df_filter_date['file_name']:
            datei=i.split('_')[5][0:8]
            date_str=datei[0:4]+'.'+datei[4:6]+'.'+datei[6:8]
            url=f'https://e4ftl01.cr.usgs.gov/ECOSTRESS/ECO2LSTE.001/{date_str}/' + i

            url_list.append(url)
        df_filter_date['url']=url_list
        T.save_df(df_filter_date, join(fdir, 'extraction_result_western_US_all_date_selection.df'))

        T.df_to_excel(df_filter_date, join(fdir, 'extraction_result_western_US_all_date_selection.xlsx'))


    def gen_hdf5_url_list(self):
        fdir = join(self.datadir, 'xml_extract')

        dff = join(fdir, 'extraction_result_western_US_all_date_selection.df')
        df = T.load_df(dff)
        url_list = df['url'].to_list()
        outf = join(fdir, 'url_list.txt')
        with open(outf, 'w') as f:
            for i in url_list:
                f.write(i + '\n')


    def data_download(self):
        fdir = join(self.datadir, 'xml_extract')
        outdir = join(self.datadir, 'dataset')
        T.mk_dir(outdir, force=True)
        f = join(fdir, 'url_list.txt')
        fr = open(f, 'r')
        url_list_r = fr.readlines()
        url_list = []
        for i in url_list_r:
            url_list.append(i.strip())

        params_list=[]
        for url in url_list:

            fname = url.split('/')[-1]
            date = fname.split('_')[-3]
            date = date.split('T')[0]
            outdir_i = join(outdir, date)
            outf = join(outdir_i, fname)
            if isfile(outf):
                continue
            # print(outf)
            # print(isfile(outf))
            # exit()
            param=(url,outdir_i,outf)
            params_list.append(param)
        MULTIPROCESS(self.download_hdf_i, params_list).run(process=10,process_or_thread='t')


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
        outf = join(outdir, 'extraction_result_all.df')
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

                        list_size.append(size)
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
        f= join(fdir, 'extraction_result_all.df')
        outf= join(fdir, 'extraction_result_western_US_all.df')

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
            check_list=[]

            for i in tqdm(range(len(all_list_i))):
                x_y_list_i=all_list_i[i]
                x=x_y_list_i[0]
                y=x_y_list_i[1]
                x_list.append(x)
                y_list.append(y)
            check_list=(self.check(x_list,y_list))
            check_all.append(check_list)
        array_check=np.array(check_all)
        array_check_T=array_check.T

        for i in range(len(array_check_T)):

            if True in array_check_T[i]:
                is_in_shapefile.append('True')
            else:
                is_in_shapefile.append('False')

        df['is_in_shapefile']=is_in_shapefile
        T.print_head_n(df)
        T.save_df(df, outf)
        T.df_to_excel(df, outf.replace('.df', ''))

    def check(self, x_list, y_list): # check if images are in the shapefile range
        poly=sb.get_poly(shpfile)

        check_list=[]

        for i in range(len(x_list)):

            x=float(x_list[i])
            y=float(y_list[i])

            check=sb.check_xys_in_poly(x, y, poly,)

            check_list.append(check)
        return check_list

    def statistic_anaysis_temporal(self):
        fdir = join(self.datadir, 'xml_extract')
        f = join(fdir, 'extraction_result_western_US_all.df')
        df = T.load_df(f)
        flie_size_list=[]
        df_filter = df[df['is_in_shapefile']=='True']
        print('number of images in shapefile is: ',len(df_filter))


  #///////////////////////statistic analysis diural
        time_start_list = df['time_start']
        date_list = []
        diural_mark_list=[]
        year_month_list=[]
        morning_list=list(range(0,12))
        afternoon_list=list(range(12,24))
        for i in time_start_list:
            year_month=i.split('_')[0][0:6]
            year_month_list.append(year_month)
            date = i.split('_')[0]
            date_list.append(date)
            time_hour=i.split('_')[1][0:2]
            if int(time_hour) in morning_list:
                diural_mark_list.append('morning')
            elif int(time_hour) in afternoon_list:
                diural_mark_list.append('afternoon')
            else:
                print('error')
                print(time_hour)
                exit()

        df['date'] = date_list
        df['diural']=diural_mark_list
        df['year_month']=year_month_list
        df_filter = df[df['is_in_shapefile'] == 'True']
        ##### print afternoon ration
        # afternoon_df=df_filter[df_filter['diural']=='afternoon']
        # afternoon_df_size=len(afternoon_df)/len(df_filter)
        # print('afternoon size is: ',afternoon_df_size)
        month_list=list(range(6,11))
        year_list=list(range(2020,2022))
        date_selection_list=[]

        for i in year_list:
            int_year=int(i)
            for j in month_list:
                j=('%.2d' % j)
                date= str(int_year)+str(j)
                date_selection_list.append(date)

        df_filter_date=df_filter[df_filter['year_month'].isin(date_selection_list)]
        print('number of images in shapefile and date selection is: ',len(df_filter_date))
        date_object_list=[]
        for i in df_filter_date['date']:
            date_object=datetime.datetime.strptime(i,'%Y%m%d')
            date_object_list.append(date_object)
        df_filter_date['date_object']=date_object_list
        date_object_list_unique=list(set(date_object_list))
        date_object_list_unique.sort()
        morning_list_count=[]
        afternoon_list_count=[]
        for date_object in date_object_list_unique:
            df_filter_date_i=df_filter_date[df_filter_date['date_object']==date_object]
            df_filter_date_i_morning=df_filter_date_i[df_filter_date_i['diural']=='morning']
            df_filter_date_i_afternoon=df_filter_date_i[df_filter_date_i['diural']=='afternoon']
            morning_list_count.append(len(df_filter_date_i_morning))
            afternoon_list_count.append(len(df_filter_date_i_afternoon))
        morning_list_count_array=np.array(morning_list_count)
        afternoon_list_count_array=np.array(afternoon_list_count)
        plt.bar(date_object_list_unique,morning_list_count_array,color='blue',label='morning',alpha=0.5)
        plt.bar(date_object_list_unique,afternoon_list_count_array,color='red',label='afternoon',bottom=morning_list_count_array,alpha=0.5)
        plt.legend()
        plt.show()

        file_size = df_filter_date['size'].tolist()

        for i in range(len(file_size)):
            file_size[i]=float(file_size[i])
            flie_size_list.append(file_size[i])
        file_size_array=np.array(flie_size_list)
        total_size=np.sum(file_size_array)/1024/1024/1024/1024    #TB
        print('total size is: ',total_size)


        # groupby_date_diural=df_filter_date.groupby(['date','diural'])
        # groupby_date_diural_size=groupby_date_diural.size()
        # groupby_date_diural_size_df=groupby_date_diural_size.to_frame()
        # groupby_date_diural_size_df.columns=['count']
        pass

    def statistic_anaysis_spatial(self):
        dff = '/Volumes/NVME2T/drought_response_Wen/demo_xml_df/extraction_result_western_US_all_new.df'
        outtif = '/Volumes/NVME2T/drought_response_Wen/demo_xml_df/count.tif'
        df = T.load_df(dff)
        df = df[df['is_in_shapefile'] == 'True']
        time_start_list = df['time_start']
        date_list = []
        for i in time_start_list:
            date = i.split('_')[0]
            date_list.append(date)
        df['date'] = date_list
        # T.print_head_n(df)
        # exit()
        demo_spatial_resolution = 0.01
        T.print_head_n(df)
        flag = 0
        x_min = 999
        x_max = -999
        y_min = 999
        y_max = -999
        for i, row in tqdm(df.iterrows(), total=len(df)):
            spatial_upperleft = row['spatial_upperleft']
            spatial_upperright = row['spatial_upperright']
            spatial_lowerleft = row['spatial_lowerleft']
            spatial_lowerright = row['spatial_lowerright']
            spatial_resolution = demo_spatial_resolution
            x1, y1 = spatial_upperleft
            x2, y2 = spatial_upperright
            x3, y3 = spatial_lowerleft
            x4, y4 = spatial_lowerright
            x_min = min(x_min, x1, x2, x3, x4)
            x_max = max(x_max, x1, x2, x3, x4)
            y_min = min(y_min, y1, y2, y3, y4)
            y_max = max(y_max, y1, y2, y3, y4)
        # print(x_min,x_max,y_min,y_max)
        # exit()
        D = DIC_and_TIF(originX=x_min,
                        endX=x_max,
                        originY=y_max,
                        endY=y_min,
                        pixelsize=demo_spatial_resolution, )
        spatial_dict = D.void_spatial_dic_zero()
        void_arr = D.pix_dic_to_spatial_arr(spatial_dict)
        # print(np.shape(void_arr))
        # exit()
        flag = 0
        for i, row in tqdm(df.iterrows(), total=len(df)):
            spatial_upperleft = row['spatial_upperleft']
            spatial_upperright = row['spatial_upperright']
            spatial_lowerleft = row['spatial_lowerleft']
            spatial_lowerright = row['spatial_lowerright']
            spatial_resolution = demo_spatial_resolution
            x1, y1 = spatial_upperleft
            x2, y2 = spatial_upperright
            x3, y3 = spatial_lowerleft
            x4, y4 = spatial_lowerright
            pix_upperleft = D.lon_lat_to_pix([x1], [y1])[0]
            pix_upperright = D.lon_lat_to_pix([x2], [y2])[0]
            pix_lowerleft = D.lon_lat_to_pix([x3], [y3])[0]
            pix_lowerright = D.lon_lat_to_pix([x4], [y4])[0]
            # print(pix_upperleft,pix_upperright,pix_lowerleft,pix_lowerright)
            # exit()
            x_list = list(range(pix_upperleft[1], pix_upperright[1]))
            y_list = list(range(pix_upperleft[0], pix_lowerleft[0]))
            # print(x_list)
            # print(y_list)
            xx, yy = np.meshgrid(x_list, y_list)
            void_arr[yy, xx] += 1
            flag += 1

        void_arr[void_arr == 0] = np.nan
        plt.imshow(void_arr)
        plt.show()
        D.arr_to_tif(void_arr, outtif)

        pass

class demo_WKG():
    def __init__(self):
        self.datadir = T.path_join(data_root, 'WKG_site')
        T.mk_dir(self.datadir)
        pass

    def run(self):
        self.extract_data_from_WKG()
        pass
    def extract_data_from_WKG(self):
        fdir=self.datadir
        f=fdir+'/WKG_LST.xlsx'
        df=pd.read_excel(f)
        print(len(df))
        T.print_head_n(df)
        # df_extact=df[df['ECO2LSTE_001_SDS_QC_Mandatory_QA_flags']!='0b11'and df['ECO2LSTE_001_SDS_QC_Mandatory_QA_flags']!='0b10']
        df_extract=df[df['ECO2LSTE_001_SDS_QC_Mandatory_QA_flags']!='0b11'] # pixel are not produced
        df_extract_noloudy = df_extract[df_extract['ECO2LSTE_001_SDS_QC_Mandatory_QA_flags'] != '0b10']  #cloudy
        print(len(df_extract_noloudy))
        ### extract date
        year_list = list(range(2021, 2022))
        month_list = list(range(7, 9))
        data_date_list=df_extract_noloudy['Date'].tolist()
        data_date_month_year_list=[]
        date_selection_list=[]

        for i in year_list:
            int_year = int(i)
            for j in month_list:
                j = ('%.2d' % j)
                date = str(int_year) + str(j)
                date_selection_list.append(date)

        diural_mark_list=[]
        morning_list=list(range(0,11))
        afternoon_list=list(range(11,24))

        for i in data_date_list:
            data_month = i.split('-')[1]
            data_year = i.split('-')[0]
            time_hour=i.split(' ')[1][0:2]
            if int(time_hour) in morning_list:
                diural_mark_list.append('morning')

            elif int(time_hour) in afternoon_list:
                diural_mark_list.append('afternoon')
            else:
                print('error')

            data_date_month_year_list.append(data_year+data_month)
        df_extract_noloudy['year_month']=data_date_month_year_list
        df_extract_noloudy['diural_mark']=diural_mark_list

        df_filter_date = df_extract_noloudy[df_extract_noloudy['year_month'].isin(date_selection_list)]
        print('number of images in shapefile and date selection is: ', len(df_filter_date))

        morning_list_count=[]
        afternoon_list_count=[]
        df_filter_date_morning=df_filter_date[df_filter_date['diural_mark']=='morning']
        morning_LST=df_filter_date_morning['ECO2LSTE_001_SDS_LST'].tolist()

        df_filter_date_afternoon=df_filter_date[df_filter_date['diural_mark']=='afternoon']
        late_lst=df_filter_date_afternoon['ECO2LSTE_001_SDS_LST'].tolist()
        plt.boxplot([morning_LST,late_lst])
        plt.ylim(270,330)

        plt.show()




    pass

def main():
    # ECOSTRESS_download().run()
    # Download_ECO().run()
    demo_WKG().run()
    pass


if __name__ == '__main__':

    main()