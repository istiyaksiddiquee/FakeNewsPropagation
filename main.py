import os
import pickle
import warnings
import pandas as pd
import networkx as nx
from tqdm import tqdm
from time import time

from feature_extractor import FeatureExtractor

warnings.simplefilter(action='ignore')

""""
steps for data extraction

# base folder + output_dir + 1-72
# load the contents of the folder as array 
# create a dataframe for each folder as you navigate them
# read each file 
# files are maps that contain the graph 
# extract all the metrics from this graph 
# append the result in the dataframe for the timebucket
# write the dataframe to a csv file on the output_dir folder
"""

def processor():

    BASE_DIR = './'
    output_dir = BASE_DIR + 'output_dir/'

    start_point = 0 
    end_point = 20

    for sub_folder_id in range(start_point, end_point + 1):
        start_timestamp = time()
        df = pd.DataFrame({
                'depth' : [], 
                'size' : [], 
                'max_breadth' : [], 
                'virality' : [], 
                'strongly_cc' : [], 
                'weakly_cc' : [], 
                'size_of_scc' : [], 
                'avg_cluster_coef' : [], 
                'density' : [], 
                'layer_ratio' : [], 
                'structural_heterogeneity' : [], 
                'characteristic_distance' : [], 
            })
        sub_folder = output_dir + str(sub_folder_id)
        files_in_sub = os.listdir(sub_folder)
        for file_addr in tqdm(files_in_sub):

            graph = None
            root_tid, _ = str(file_addr).split('_')
            with open(sub_folder + '/' + str(file_addr), 'rb') as handle:
                graph = pickle.load(handle)
            
            
            feature_extrator = FeatureExtractor(graph=graph)
            df.loc[len(df.index)] = list(feature_extrator.extract_features(root=int(root_tid)))

        df.to_csv(output_dir + str(sub_folder_id)+'.csv', sep=',', encoding='utf-8')
        del df
        
        end_timestamp = time()

        print("{} folder took : {} minutes".format(sub_folder_id, (start_timestamp-end_timestamp)/60))

if __name__ == "__main__":
    processor()