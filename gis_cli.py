# -*- coding: utf-8 -*-
"""
Created on Sun Feb 21 18:10:32 2021

@author: Camargo
"""

# https://developers.arcgis.com/python/sample-notebooks/publishing-sd-shapefiles-and-csv/
# https://developers.arcgis.com/python/guide/accessing-and-creating-content/
# https://developers.arcgis.com/python/api-reference/arcgis.gis.toc.html#arcgis.gis.Item.publish
# https://developers.arcgis.com/python/api-reference/arcgis.gis.toc.html#gis

import argparse
import os
from src.arcgis_api import *


def _parse_args():
    parser = argparse.ArgumentParser(description='i-team arcgis api wrapper')

    # Not yet implemented.
    #
    # parser.add_argument('-v', '--verbose',
    #                     action='store_const',
    #                     const=True, default=False,
    #                     help='keep you updated as stuff happens')
        
    parser.add_argument('--geocode', '-gc',
                        help='Example: python gis-cli.py -gc "data/addresses.csv"',
                        type=str)
    
    parser.add_argument('--upload_feature_layer', '-ufl',
                        help='Example: python gis-cli.py -ufl "output/geocoded_addresses.csv"',
                        type=str)
                        
    parser.add_argument('--enrich', '-e',
                        help='Example: python gis-cli.py -e "Arcgis Feature Layer Name"',
                        type=str)
                                          
    parser.add_argument('--download_feature_layer', '-dfl',
                    help='Example: python gis-cli.py -dfl "6a57ff95150f404d884bd782f690d7e6"',
                    type=str)
                    
    parser.add_argument('--race_breakdown', '-rb',
                    help='Example: python gis-cli.py -rb "output/Enriched Feature Layer.csv"',
                    type=str)
    
    return parser.parse_args()

def main():
    """Access Arcgis Online API"""
    args = _parse_args()
    
    arcgis = arcgis_api()

    # if args.filename is None:
    #     print('No filename provided')
    #     return
    
    if args.geocode is not None:
        arcgis.geocode_csv(args.geocode)
        return
    
    if args.upload_feature_layer is not None:
        arcgis.upload_as_feature_layer(args.upload_feature_layer)
        return
        
    if args.enrich is not None:
        arcgis.enrich(args.enrich)
        return
        
    if args.download_feature_layer is not None:
        arcgis.download_feature_layer(args.download_feature_layer)
        return
        
    if args.race_breakdown is not None:
        arcgis.print_breakdown(args.race_breakdown)
        return


if __name__ == '__main__':
    main()
