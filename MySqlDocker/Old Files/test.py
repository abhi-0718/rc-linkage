# from splink.duckdb.duckdb_linker import DuckDBLinker
# import splink.duckdb.duckdb_comparison_library as cl
# from splink.duckdb.duckdb_comparison_library import (
#     exact_match,
#     levenshtein_at_thresholds,
#     jaro_winkler_at_thresholds
# )
# import splink.duckdb.duckdb_comparison_template_library as ctl
# import pandas as pd

# df = pd.read_excel(r"Dataset\Electronic_Health_Record.xlsx")
# df['unique_id'] = [i for i in range(1,len(df)+1)]
# df
# settings = {
# "link_type": "dedupe_only",
#     "blocking_rules_to_generate_predictions": [

#         "l.given_name = r.given_name",
#         "l.surname = r.surname"
        
#     ],
#     "comparisons": [

#             ctl.name_comparison("given_name"),
#             ctl.name_comparison("surname"),
#             levenshtein_at_thresholds("birthdate",[1,2]),
#             jaro_winkler_at_thresholds("address",[0.8,0.75]),
#             jaro_winkler_at_thresholds("county",[0.8,0.75]),
#             levenshtein_at_thresholds("city",2),
#             levenshtein_at_thresholds("state",2),
#             exact_match("zip", term_frequency_adjustments = True)

#     ],
# }

# linker = DuckDBLinker(df,settings)
# linker.estimate_u_using_random_sampling(max_pairs=1e6)

# blocking_rule_for_training = "l.given_name = r.given_name and l.surname = r.surname"
# linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

# # blocking_rule_for_training = "l.zip = r.zip"
# # linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

# pairwise_predictions = linker.predict()

# clusters = linker.cluster_pairwise_predictions_at_threshold(pairwise_predictions, 0.75)

# dataframe = clusters.as_pandas_dataframe()
# dataframe
# print(dataframe)

import yaml
from yaml.loader import SafeLoader

# Open the file and load the file
with open('config.yaml') as f:
    data = yaml.load(f, Loader=SafeLoader)
print(data)