from preprocessing import Preprocess
import pandas as pd
import json
from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
import splink.duckdb.duckdb_comparison_template_library as ctl
from splink.duckdb.duckdb_comparison_library import (
    exact_match,
    levenshtein_at_thresholds,
    jaro_winkler_at_thresholds
)
import vl_convert as vlc

columns_to_encrypt_usa = ['given_name', 'surname', 'address_1', 'address_2', 'city', 'state', 'zip', 'latitude', 'longitude']

class Splink_linkage():

    def __init__(self):

        try:
            with open('path.json','r') as config:
                self.params = json.load(config)

        except Exception as err:
            print(err)

    def Deduplication(self, main_dataframe, label_exist, check_which_encryption):
        
        def febrl_deduplication(dataframe):
            settings = {
            "link_type": "dedupe_only",
                "blocking_rules_to_generate_predictions": [
                    "l.surname = r.surname",
                    "l.postcode = r.postcode"
                ],
                "comparisons": [
                    ctl.name_comparison("given_name"),
                    ctl.name_comparison("surname"),
                    levenshtein_at_thresholds("date_of_birth",[1,2]),
                    jaro_winkler_at_thresholds("address_1",[0.9,0.7]),
                    jaro_winkler_at_thresholds("address_2",[0.9,0.7]),
                    levenshtein_at_thresholds("suburb",2),
                    levenshtein_at_thresholds("state",2),
                    exact_match("street_number", term_frequency_adjustments = True),
                    exact_match("postcode", term_frequency_adjustments = True)
                ],
            }

            linker = DuckDBLinker(dataframe,settings)
            linker.estimate_u_using_random_sampling(max_pairs=1e6)

            blocking_rule_for_training = "l.given_name = r.given_name and l.surname = r.surname"
            linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

            blocking_rule_for_training = "l.date_of_birth = r.date_of_birth"
            linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)
            return linker


        def usa_deduplication( dataframe):
            
            settings = {
            "link_type": "dedupe_only",
                "blocking_rules_to_generate_predictions": [

                    "l.given_name = r.given_name",
                    "l.surname = r.surname"
                    
                ],
                "comparisons": [

                        ctl.name_comparison("given_name"),
                        ctl.name_comparison("surname"),
                        levenshtein_at_thresholds("birthdate",[1,2]),
                        jaro_winkler_at_thresholds("address",[0.8,0.75]),
                        levenshtein_at_thresholds("city",2),
                        levenshtein_at_thresholds("state",2),
                        exact_match("zip", term_frequency_adjustments = True)

                ],
            }

            linker = DuckDBLinker(dataframe,settings)
            linker.estimate_u_using_random_sampling(max_pairs=1e6)
            print('- Random Sampling -')
            blocking_rule_for_training = "l.given_name = r.given_name and l.surname = r.surname"
            linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)
            
            return linker 
        
        process = Preprocess()
        main_dataframe = process.Decrpyter(main_dataframe,check_which_encryption)
        main_dataframe['unique_id'] = [x for x in range(1,len(main_dataframe)+1)]

        if check_which_encryption == 'febrl':
            linker = febrl_deduplication(main_dataframe)

            if label_exist==1:

                spec = linker.roc_chart_from_labels_column("cluster").spec
                png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
                with open(self.params['base_febrl_deduplication']+self.params['roc_auc_chart'], "wb") as f:
                    f.write(png_data)

                spec = linker.precision_recall_chart_from_labels_column("cluster").spec
                png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
                with open(self.params['base_febrl_deduplication']+self.params['precision_recall'], "wb") as f:
                    f.write(png_data)

            spec = linker.match_weights_chart().spec
            png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
            with open(self.params['base_febrl_deduplication']+self.params['match_weight'], "wb") as f:
                f.write(png_data)

            spec = linker.m_u_parameters_chart().spec
            png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
            with open(self.params['base_febrl_deduplication']+self.params['mu_parameters'], "wb") as f:
                f.write(png_data)

        else:
        
            linker = usa_deduplication(main_dataframe)
            
            if label_exist==1:

                spec = linker.roc_chart_from_labels_column("cluster").spec
                png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
                with open(self.params['base_custom_deduplication']+self.params['roc_auc_chart'], "wb") as f:
                    f.write(png_data)

                spec = linker.precision_recall_chart_from_labels_column("cluster").spec
                png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
                with open(self.params['base_custom_deduplication']+self.params['precision_recall'], "wb") as f:
                    f.write(png_data)

            spec = linker.match_weights_chart().spec
            png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
            with open(self.params['base_custom_deduplication']+self.params['match_weight'], "wb") as f:
                f.write(png_data)

            spec = linker.m_u_parameters_chart().spec
            png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
            with open(self.params['base_custom_deduplication']+self.params['mu_parameters'], "wb") as f:
                f.write(png_data)


        pairwise_predictions = linker.predict()
        clusters = linker.cluster_pairwise_predictions_at_threshold(pairwise_predictions, 0.70)
        resulted_dataframe = clusters.as_pandas_dataframe()


        return resulted_dataframe
    

    def Linking( self, dataframeA, dataframeB, labels_exist, check_which_encryption):

                
        def febrl_linkage(dataframes):

            settings_dictionary = {

            "link_type": "link_only",

            "blocking_rules_to_generate_predictions": [

            "l.given_name = r.given_name AND l.surname = r.surname",
            "l.date_of_birth = r.date_of_birth",
            "l.soc_sec_id = r.soc_sec_id"
            
            ],

            "comparisons": [

                ctl.name_comparison("given_name", term_frequency_adjustments_name=True),
                ctl.name_comparison("surname", term_frequency_adjustments_name=True),
                cl.levenshtein_at_thresholds("date_of_birth", [1, 2]),
                cl.levenshtein_at_thresholds("soc_sec_id", [1, 2]),
                cl.exact_match("street_number", term_frequency_adjustments=True),
                cl.levenshtein_at_thresholds("postcode", [1, 2], term_frequency_adjustments=True),
               
            ],

            "retain_intermediate_calculation_columns": True,

            }
            
            linker = DuckDBLinker(dataframes,settings_dictionary)
            linker.estimate_u_using_random_sampling(max_pairs=1e6)

            linker.estimate_parameters_using_expectation_maximisation("l.date_of_birth = r.date_of_birth")
            linker.estimate_parameters_using_expectation_maximisation("l.postcode = r.postcode")

            return linker
            
        def usa_linkage( dataframes):
            
            settings = {
            "link_type": "link_only",
                "blocking_rules_to_generate_predictions": [

                    "l.given_name = r.given_name",
                    "l.surname = r.surname"
                    
                ],
                "comparisons": [

                    ctl.name_comparison("given_name"),
                    ctl.name_comparison("surname"),
                    levenshtein_at_thresholds("birthdate",[1,2]),
                    jaro_winkler_at_thresholds("address",[0.8,0.75]),
                    levenshtein_at_thresholds("city",2),
                    levenshtein_at_thresholds("state",2),
                    exact_match("zip", term_frequency_adjustments = True)

                ],
            }

            linker = DuckDBLinker(dataframes,settings)
            linker.estimate_u_using_random_sampling(max_pairs=1e6)
            print('- Random Sampling -')
            blocking_rule_for_training = "l.given_name = r.given_name and l.surname = r.surname"
            linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)
            
            return linker 
        
        process = Preprocess()
        dataframeA = process.Decrpyter( dataframeA, check_which_encryption)
        dataframeB = process.Decrpyter( dataframeB, check_which_encryption)

        dataframeA['unique_id'] = [x for x in range(1,len(dataframeA)+1)]
        dataframeB['unique_id'] = [x for x in range(1,len(dataframeB)+1)]

        dataframes = [dataframeA,dataframeB]
        
        if check_which_encryption == 'febrl':
            linker = febrl_linkage(dataframes)

            if labels_exist==1:

                spec = linker.roc_chart_from_labels_column("cluster").spec
                png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
                with open(self.params['base_febrl_linkage']+self.params['roc_auc_chart'], "wb") as f:
                    f.write(png_data)

                spec = linker.precision_recall_chart_from_labels_column("cluster").spec
                png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
                with open(self.params['base_febrl_linkage']+self.params['precision_recall'], "wb") as f:
                    f.write(png_data)

            spec = linker.match_weights_chart().spec
            png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
            with open(self.params['base_febrl_linkage']+self.params['match_weight'], "wb") as f:
                f.write(png_data)

            spec = linker.m_u_parameters_chart().spec
            png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
            with open(self.params['base_febrl_linkage']+self.params['mu_parameters'], "wb") as f:
                f.write(png_data)

        else:
            linker = usa_linkage(dataframes)

            if labels_exist==1:

                spec = linker.roc_chart_from_labels_column("cluster").spec
                png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
                with open(self.params['base_custom_linkage']+self.params['roc_auc_chart'], "wb") as f:
                    f.write(png_data)

                spec = linker.precision_recall_chart_from_labels_column("cluster").spec
                png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
                with open(self.params['base_custom_linkage']+self.params['precision_recall'], "wb") as f:
                    f.write(png_data)

            spec = linker.match_weights_chart().spec
            png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
            with open(self.params['base_custom_linkage']+self.params['match_weight'], "wb") as f:
                f.write(png_data)

            spec = linker.m_u_parameters_chart().spec
            png_data = vlc.vegalite_to_png(vl_spec=spec, scale=2)
            with open(self.params['base_custom_linkage']+self.params['mu_parameters'], "wb") as f:
                f.write(png_data)

        pairwise_predictions = linker.predict()
        clusters = linker.cluster_pairwise_predictions_at_threshold(pairwise_predictions, 0.70)
        resulted_dataframe = clusters.as_pandas_dataframe()


        return resulted_dataframe







            
            


