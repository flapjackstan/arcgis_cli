import yaml # handles login keys
import tokenize # handles token errors
import json 
import shutil # file manager
import pickle # python data structure loader

import geopandas as gpd
import pandas as pd
from arcgis.gis import GIS # login manager'
from arcgis.geocoding import geocode, batch_geocode , get_geocoders# geocode
from arcgis.features import FeatureLayer # access layer
from arcgis.features import enrich_data
from arcgis.geoenrichment import Country


class arcgis_api(object):
    """
    Python class for common arcgis workflows 
    """
    def __init__(self):

        self.user_name = "username"
        self.password = "password"

    def read_df(self, csv_path: str):
        """
        Reads csv from path

        Args:
            csv_path (str): path to csv that you want to geocode

        example: ../data/Mobile Sites Feb 19.csv
        """
        print('/'.join(csv_path.split("/")[:-1]))
        return 
    
    def connect_gis(self):
        """
        Connects to arcgis online account

        Args:
            yaml file with keys must be outside of directory

        example: see yaml example
        """
        with open(r'../keys.yaml') as file:
            	keys = yaml.load(file, Loader=yaml.FullLoader)
            
        try:
            gis = GIS(url="http://lahub.maps.arcgis.com/home/organization.html",
                      username=keys['arcgis_username'],password=keys['arcgis_password'])
            
            print("Successful Connection to AGOL API: ",type(gis.content))
            
        except tokenize.TokenError:
            pass
        
        except:
            print('Other Error')
            
        return gis

    def geocode_csv(self, csv_path: str):
        """
        Geocodes 'Address' column in a csv and returns the same csv with lat longs in the output folder

        Args:
            csv_path (str): path to csv that you want to geocode

        Example: ../data/Mobile Sites Feb 19.csv
        
        To Do: Provide geocoded results such as 19/19 or 12/19 locations successfully geocoded 
        """
        
        gis = self.connect_gis()    
        
        # uses world geocode server (most accurate)
        geocoder = get_geocoders(gis)
        
        file_name = csv_path.split("/")[-1]
        df = pd.read_csv(csv_path)
        addy = df.Address.to_list()
        results = batch_geocode(addy, source_country="USA", geocoder=geocoder[1])
        
        location_df = pd.DataFrame()

        for i in results:
            results_json = pd.json_normalize(i['attributes'])
            location_df = location_df.append(results_json)
        
        df = pd.merge(df,location_df[['ResultID','X','Y']],left_index=True,right_index=False, right_on='ResultID')

        df.to_csv('output/csv/Geocoded '+ file_name, index=False)
        print('Saved Geocoded Data To:','output/csv/Geocoded '+ file_name)
        return
    
    def geocode_df(self, df):
        """
        Geocodes 'Address' column in a df and returns the same df with lat longs 

        Args:
            df: df that you want to geocode

        Example: geocoded_df = gis.geocode_df(non_geocoded_df)
        """
        
        gis = self.connect_gis()    
        
        # uses world geocode server (most accurate)
        geocoder = get_geocoders(gis)
                
        addy = df.Address.to_list()
        results = batch_geocode(addy, source_country="USA", geocoder=geocoder[1])
        
        location_df = pd.DataFrame()

        for i in results:
            results_json = pd.json_normalize(i['attributes'])
            location_df = location_df.append(results_json)
        
        df = pd.merge(df,location_df[['ResultID','X','Y']],left_index=True,right_index=False, right_on='ResultID')

        return df
    
    def mass_geocode_df(self, df):
        """
        Geocodes 'Address' column in a df with more than 1000 addresses and returns the same df with lat longs 

        Args:
            df: df that you want to geocode

        Example: geocoded_df = gis.mass_geocode_df(non_geocoded_df)
        """
        
        ngc_json = df.to_json(orient='records')

        # read string as json
        ngc_json = json.loads(ngc_json)
        
        gis = self.connect_gis()    
        
        # uses world geocode server (most accurate)
        geocoder = get_geocoders(gis)
        
        count = 0
        for i in range(len(ngc_json)):
            address = ngc_json[count]["Address"]
            
            # geocode
            address_geocoded = geocode(address, source_country="USA", geocoder=geocoder[1])
            
            #string to json
            address_json = json.dumps(address_geocoded)
            
            #load json
            address_json = json.loads(address_json)
            
            #assign geocoded json to original dfjson
            ngc_json[count]['location'] = address_json[0]['location']
            
            count += 1
            print(count)
            
        gc_df = pd.json_normalize(ngc_json)
                
        return gc_df
    
    
    def shp_zip(self, gdf,shp_dir,file_name):   
        try:
            gdf.to_file(shp_dir + "/" + file_name, driver = 'ESRI Shapefile')
        except OSError as error:  
            print(error) 
            
        shutil.make_archive(shp_dir + "/" + file_name, 'zip', shp_dir + "/" + file_name)
        
    def upload_as_feature_layer(self, file_path: str):
        """
        Uploads either a shapefile or csv with coordinates as a feature layer

        Args:
            file_path (str): path to file that you want to upload

        example: ../output/Geocoded Mobile Sites Feb 19.csv
                 ../output/Geocoded Mobile Sites Feb 19.shp
        """
        
        gis = self.connect_gis()    
        
        full_file_name = file_path.split("/")[-1]
        #path = '/'.join(file_path.split("/")[:-1])
        
        file_name = full_file_name[:-4]
        #file_type = file_name[-4:]
       
        # makes feaure layer that is not hosted 
        # if full_file_name.endswith('.csv'):
        #     df = pd.read_csv(file_path) 
        #     feature_layer = gis.content.import_data(df,location_type='coordinates', latitude_field='Y', longitude_field='X')

        #     feature_layer_dict = dict(feature_layer.properties)
        #     feature_layer_json = json.dumps({"featureCollection": {"layers": [feature_layer_dict]}})
            
        #     feature_layer_properties = {'title': file_name,
        #                             'description':'Mobile Vaccine Sites Carbon Health',
        #                             'tags': 'vaccine, i-team',
        #                             'text':feature_layer_json,
        #                             'type':'Feature Collection'}
            
        #     feature_layer_item = gis.content.add(feature_layer_properties)
        #     print(feature_layer_item)
        
        
        if full_file_name.endswith('.csv'):
            #csv_file = file_path
            
            # pub_params = {"locationType":"coordinates",
            #               "latitudeFieldName" : "Y",
            #               "longitudeFieldName" : 'X'}
            
            df = pd.read_csv(file_path) 
            feature_layer = gis.content.import_data(df,location_type='coordinates', latitude_field='Y', longitude_field='X')
            feature_layer_dict = dict(feature_layer.properties)
            feature_layer_json = json.dumps({"featureCollection": {"layers": [feature_layer_dict]}})
            
            feature_layer_properties = {'title': file_name,
                                   'description':'Mobile Vaccine Sites Carbon Health',
                                   'tags': 'vaccine, i-team',
                                   'text':feature_layer_json,
                                   'type':'Feature Collection'}
            
            #csv_item = gis.content.add(feature_layer_properties, csv_file)
            #csv_lyr = csv_item.publish(None, pub_params)
            csv_item = gis.content.add(feature_layer_properties)
            csv_lyr = csv_item.publish()
            print(csv_lyr)

        elif full_file_name.endswith('.shp'):
            print('This file is a shapefile. still working on that')
            gdf = gpd.read_file(file_path)
            
            self.shp_zip(gdf,'output',file_name)
            print('output' + "/" + file_name)
            
            shpfile = gis.content.add({}, 'output' + "/" + file_name)
            published_service = shpfile.publish()
            
            item_properties = {"title":file_name,
                               'description':'Vaccine Sites Carbon Health',
                               "tags":"vaccine, i-team"}
            
            published_service.update(item_properties)
            print(published_service)
         
        elif full_file_name.endswith('.zip'):

            shpfile = gis.content.add({}, file_path)
            published_service = shpfile.publish()
            
            item_properties = {"title":file_name,
                               'description':'Vaccine Sites Carbon Health',
                               "tags":"vaccine, i-team"}
            
            published_service.update(item_properties)
            print(published_service)
        return 

    def prepend(self,list,str): 
            # Using format() 
            str += '{0}'
            list = [str.format(i) for i in list] 
            return(list) 

    def enrich(self, feature_layer_name: str):
        """
        Takes a hosted feature layer and buffers the layer and apportions/enriches the newely created layer with ESRI variables 

        Args:
            feature_layer_name (str): Name of hosted feature layer

        example: "Geocoded Mobile Sites Mar 1"
        
        todo
        append data automatically
        """
        
        gis = self.connect_gis()    
        
        search_result = gis.content.search('title:' + feature_layer_name, item_type='Feature Layer')
        feature_layer = gis.content.get(search_result[0].id)
        
        # grab first feature layer from feature layer collection 
        layer = FeatureLayer(feature_layer.layers[0].url)
        
        usa = Country.get('US')
        
        datasets = usa.data_collections

        # get all the unique data collections available
        topics = datasets.index.unique()

        with open ('data/enrichment_variables/race_variables.txt', 'rb') as fp:
            race_variables = pickle.load(fp)
    
        race_variables = self.prepend(race_variables, 'agebyracebysex.') 
        
        race_layer_name = 'Age and Race by Sex 3 Mile Buffer ' + feature_layer_name 
        print('Enriching Data...Please Wait')
        race_buffer = enrich_data.enrich_layer(layer, country='US', analysis_variables=race_variables, buffer_type='StraightLine', distance=3, units='Miles', output_name=race_layer_name)
        print('Newely Created Layer ID is:',race_buffer.id)

    def download_feature_layer(self, feature_layer_id: str):
        """
        Downloads a feature layer 

        Args:
            feature_layer_id (str): ID of hosted feature layer

        example: "6a57ff95150f404d884bd782f690d7e6"
        
        todo:
        add flag for shapefile
        """
        
        gis = self.connect_gis()    
        
        # pull content from layer
        feature_layer = gis.content.get(feature_layer_id)

        print('Feature Layers Associated',feature_layer.layers)

        # grab first feature layer from feature layer collection 
        layer = FeatureLayer(feature_layer.layers[0].url)
        layer_name = feature_layer.layers[0].properties.name

        # print layer capabilities
        layer.properties.capabilities

        df = pd.DataFrame((layer.query(out_fields='*', where='1=1', as_df=True, return_geometry=False)))
        df.to_csv('output/csv/'+layer_name+'.csv', index=False)
        print('Data saved to '+'output/csv.'+layer_name+'.csv')

    def print_breakdown(self, file_path: str):
        """
        Prints race age breakdown 

        Args:
            file_path (str): Path to csv output for race age breakdown

        example: "output/Age and Race by Sex 3 Mile Buffer Mar 1.csv"
        """
        
        df = pd.read_csv(file_path)
        ## White
        print('Total White Pop:',sum(df['WAGEBASECY']))
        print('Total White Male Pop:',sum(df['WHTMBASECY']))

        df['WM65-74'] = df.WHTM65_CY + df.WHTM70_CY
        print('Total White Males 65-74:',sum(df['WM65-74']))

        df['WM75-84'] = df.WHTM75_CY + df.WHTM80_CY
        print('Total White Males 75-84:',sum(df['WM75-84']))

        print('Total White Males 85+:',sum(df['WHTM85_CY']))

        print('Total White Female Pop:',sum(df['WHTFBASECY']))


        df['WF65-74'] = df.WHTF65_CY + df.WHTF70_CY
        print('Total White Females 65-74:',sum(df['WF65-74']))

        df['WF75-84'] = df.WHTF75_CY + df.WHTF80_CY
        print('Total White Females 75-84:',sum(df['WF75-84']))

        print('Total White Females 85+:',sum(df['WHTF85_CY']))


        input('Press Any Key To Continue')
        ## Black

        print('Total Black Pop:',sum(df['WAGEBASECY']))
        print('Total Black Male Pop:',sum(df['BLKMBASECY']))

        df['BLKM65-74'] = df.BLKM65_CY + df.BLKM70_CY
        print('Total Black Males 65-74:',sum(df['BLKM65-74']))

        df['BLKM75-84'] = df.BLKM75_CY + df.BLKM80_CY
        print('Total Black Males 75-84:',sum(df['BLKM75-84']))

        print('Total Black Males 85+:',sum(df['BLKM85_CY']))


        print('Total Black Female Pop:',sum(df['BLKFBASECY']))

        df['BLKF65-74'] = df.BLKF65_CY + df.BLKF70_CY
        print('Total Black Females 65-74:',sum(df['BLKF65-74']))

        df['BLKF75-84'] = df.BLKF75_CY + df.BLKF80_CY
        print('Total Black Females 75-84:',sum(df['BLKF75-84']))

        print('Total Black Females 85+:',sum(df['BLKF85_CY']))

        input('Press Any Key To Continue')

        ## Native American

        print('Total Native American Pop:',sum(df['IAGEBASECY']))

        print('Total Native American Male Pop:',sum(df['AIMBASE_CY']))

        df['AIM65-74'] = df.AIM65_CY + df.AIM70_CY
        print('Total Native American Males 65-74:',sum(df['AIM65-74']))

        df['AIM75-84'] = df.AIM75_CY + df.AIM80_CY
        print('Total Native American Males 75-84:',sum(df['AIM75-84']))

        print('Total Native American Males 85+:',sum(df['AIM85_CY']))

        print('Total Native American Female Pop:',sum(df['AIFBASE_CY']))

        df['AIF65-74'] = df.AIF65_CY + df.AIF70_CY
        print('Total Native American Females 65-74:',sum(df['AIF65-74']))

        df['AIF75-84'] = df.AIF75_CY + df.AIF80_CY
        print('Total Native American Females 75-84:',sum(df['AIF75-84']))

        print('Total Native American Females 85+:',sum(df['AIF85_CY']))

        input('Press Any Key To Continue')

        ## Asian

        print('Total Asian Pop:',sum(df['AAGEBASECY']))

        print('Total Asian Male Pop:',sum(df['ASNMBASECY']))

        df['ASNM65-74'] = df.ASNM65_CY + df.ASNM70_CY
        print('Total Asian Males 65-74:',sum(df['ASNM65-74']))

        df['ASNM75-84'] = df.ASNM75_CY + df.ASNM80_CY
        print('Total Asian Males 75-84:',sum(df['ASNM75-84']))

        print('Total Asian Males 85+:',sum(df['ASNM85_CY']))

        print('Total Asian Female Pop:',sum(df['ASNFBASECY']))

        df['ASNF65-74'] = df.ASNF65_CY + df.ASNF70_CY
        print('Total Asian Females 65-74:',sum(df['ASNF65-74']))

        df['ASNF75-84'] = df.ASNF75_CY + df.ASNF80_CY
        print('Total Asian Females 75-84:',sum(df['ASNF75-84']))

        print('Total Asian Females 85+:',sum(df['ASNF85_CY']))

        input('Press Any Key To Continue')

        ## Pacific Islander

        print('Total Pacific Islander Pop:',sum(df['PAGEBASECY']))

        print('Total Pacific Islander Male Pop:',sum(df['PIMBASE_CY']))
        df['PIM65-74'] = df.PIM65_CY + df.PIM70_CY
        print('Total Pacific Islander Males 65-74:',sum(df['PIM65-74']))

        df['PIM75-84'] = df.PIM75_CY + df.PIM80_CY
        print('Total Pacific Islander Males 75-84:',sum(df['PIM75-84']))

        print('Total Pacific Islander Males 85+:',sum(df['PIM85_CY']))

        print('Total Pacific Islander Female Pop:',sum(df['PIFBASE_CY']))

        df['PIF65-74'] = df.PIF65_CY + df.PIF70_CY
        print('Total Pacific Islander Females 65-74:',sum(df['PIF65-74']))

        df['PIF75-84'] = df.PIF75_CY + df.PIF80_CY
        print('Total Pacific Islander Females 75-84:',sum(df['PIF75-84']))

        print('Total Pacific Islander Females 85+:',sum(df['PIF85_CY']))

        input('Press Any Key To Continue')

        ## Other
        print('Total Other Race Pop:',sum(df['OAGEBASECY']))

        print('Total Other Race Male Pop:',sum(df['OTHMBASECY']))

        df['OTHM65-74'] = df.OTHM65_CY + df.OTHM70_CY
        print('Total Other Race Males 65-74:',sum(df['OTHM65-74']))

        df['OTHM75-84'] = df.OTHM75_CY + df.OTHM80_CY
        print('Total Other Race Males 75-84:',sum(df['OTHM75-84']))

        print('Total Other Race Males 85+:',sum(df['OTHM85_CY']))

        print('Total Other Race Female Pop:',sum(df['OTHFBASECY']))

        df['OTHF65-74'] = df.OTHF65_CY + df.OTHF70_CY
        print('Total Other Race Females 65-74:',sum(df['OTHF65-74']))

        df['OTHF75-84'] = df.OTHF75_CY + df.OTHF80_CY
        print('Total Other Race Females 75-84:',sum(df['OTHF75-84']))

        print('Total Other Race Females 85+:',sum(df['OTHF85_CY']))

        input('Press Any Key To Continue')

        ## Hispanic

        print('Total Hispanic Pop:',sum(df['HAGEBASECY']))

        print('Total Hispanic Male Pop:',sum(df['HSPMBASECY']))

        df['HSPM65-74'] = df.HSPM65_CY + df.HSPM70_CY
        print('Total Hispanic Males 65-74:',sum(df['HSPM65-74']))

        df['HSPM75-84'] = df.HSPM75_CY + df.HSPM80_CY
        print('Total Hispanic Males 75-84:',sum(df['HSPM75-84']))

        print('Total Hispanic Males 85+:',sum(df['HSPM85_CY']))

        print('Total Hispanic Female Pop:',sum(df['HSPFBASECY']))

        df['HSPF65-74'] = df.HSPF65_CY + df.HSPF70_CY
        print('Total Hispanic Females 65-74:',sum(df['HSPF65-74']))

        df['HSPF75-84'] = df.HSPF75_CY + df.HSPF80_CY
        print('Total Hispanic Females 75-84:',sum(df['HSPF75-84']))

        print('Total Hispanic Females 85+:',sum(df['HSPF85_CY']))

        input('Press Any Key To Continue')