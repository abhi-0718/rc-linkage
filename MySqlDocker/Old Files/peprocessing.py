import pandas as pd
import enchant
import base64
import numpy as np
from spellchecker import SpellChecker
from fuzzywuzzy import fuzz
import recordlinkage as rl
from recordlinkage.index import SortedNeighbourhood

abbreviation_path = 'Dataset/Abrreviations.xlsx'


class Preprocess():

    def initialize( df, predefined_dataset):

        """
            Combines the city and state into a single string
            Args:
                df: Dataframe that the user inputs
                predefined_dataset: USA official cities and states dataset
            
            Updates:
                city_state_sample: Combination of city and state for user dataframe
                city_state_predefined : Combination of city and state for predefined dataset
        """
        try:

            city_state_sample = []
            for city in range(0, len(df)):
                sample_city = df['city'][city]
                sample_state = df['state'][city]

                string_output = str(sample_city)+','+str(sample_state)
                city_state_sample.append(string_output)

            city_state_predefined = predefined_dataset['city_state']
            city_state_predefined = list(city_state_predefined)

            return city_state_sample, city_state_predefined 

        except Exception as err:
            print(err)



    def correct_change( df, predefined_dataset, city_state_predefined):

        """
            This function corrects the cities and states from user sample based on the predefined dataset             
            Args:
                    df: Dataframe that the user inputs
                    predefined_dataset: USA official cities and states dataset
                    city_state_predefined : Combination of city and state for predefined dataset
                    city_state_sample: Combination of city and state for user dataframe
                
                Returns:

                    corrected_dataframe: This dataframe has two columns the Original and Changed, which 
                    changed has the correct values with respect to the Original
        """
        try:

            sample_cities = []
            sample_states = []
            not_present_city_state = []
            cities_corrected = {}

            sample_city = df['city']
            sample_city = list(sample_city)

            predefined_city = predefined_dataset['city']
            predefined_city = list(predefined_city)

            sample_state = df['state']
            sample_state = list(sample_state)

            predefined_state = predefined_dataset['state_id']
            predefined_state = list(predefined_state)

            for city in sample_city:
                if city not in predefined_city:
                    sample_cities.append(city)

            for state in sample_state:
                if state not in predefined_state:
                    sample_states.append(state)

            for city in range(0, len(sample_city)):

                if sample_city[city] in sample_cities:
                    string1 = sample_city[city]+','+sample_state[city]
                    not_present_city_state.append(string1)

            for state in range(0, len(sample_state)):

                if sample_state[state] in sample_states:
                    string1 = sample_city[state]+','+sample_state[state]
                    not_present_city_state.append(string1)

            not_present_city_state = set(not_present_city_state)

            minimum = 999
            for iterator_city_state in not_present_city_state:
                minimum = 999
                for correct_city_state in city_state_predefined:
                    temp = enchant.utils.levenshtein(
                        iterator_city_state, correct_city_state)
                    if temp < 3:
                        if temp < minimum:
                            minimum = temp
                            cities_corrected[iterator_city_state] = correct_city_state

                    else:
                        continue

            corrected_dataframe = pd.DataFrame()
            corrected_dataframe['Original Values'] = cities_corrected.keys()
            corrected_dataframe['Suggested Changes'] = cities_corrected.values()

            return corrected_dataframe

        except Exception as err:
            print(err)



    def preprocess_abbreviation( df):

        """
            This function corrects the abbreviations in the dataset provided by the user             
            Args:
                df: Dataframe that the user inputs
                
            Returns:

                tokens: This dataframe has two columns the Original and Changed, which 
                changed has the correct values with respect to the Original

        """
        try:
            abbreviation = pd.read_excel(abbreviation_path)
            proper_abbreviations = abbreviation['Abbreviations']
            proper_abbreviations = list(proper_abbreviations)

            correct_abbreviations = abbreviation['Corrected_Abbreviations']
            correct_abbreviations = list(correct_abbreviations)
            mapping_dict = {}
            for something in range(0, len(proper_abbreviations)):
                mapping_dict[proper_abbreviations[something]] = correct_abbreviations[something]

            # print("Mapping_dict:",mapping_dict)
            Address = df['address']
            Address = list(Address)
            tokens = []
            for part in Address:
                nltk_tokens = part.split(' ')
                tokens.append(nltk_tokens)

            for part in tokens:
                for some in range(0, len(part)):
                    if part[some] in mapping_dict.keys():
                        part[some] = mapping_dict[part[some]]

            for tok in range(0, len(tokens)):
                tokens[tok] = ' '.join(tokens[tok])

            return tokens
        
        except Exception as err:
            print(err)


    def update_values( response, dataframe):

        """
            This function corrects the abbreviations in the dataset provided by the user             
            Args:

                dataframe: Dataframe that the user inputs
                response: The updated response from the user after correction

            Returns:

                dataframe: This dataframe has all the basic abbreviations corrected, as this is 
                checked in this function
        """

        try:

            original_not_to_change = []
            changed = []
            indices = []
    
            state = list(dataframe['state'])
            city = list(dataframe['city'])

            for index in response['selected_rows']:
                original_not_to_change.append(index['Original Values'])

            original = list(response['data']['Original Values'])
            changed = list(response['data']['Suggested Changes'])

            for part in range(0, len(original)):
                
                parted_original = original[part].split(',')
                parted_changed = changed[part].split(',')
                k = True
                if original[part] in original_not_to_change:
                    continue

                else:
                    ind = (dataframe[dataframe['city']==parted_original[0]]).index.tolist()
                    indices.append(ind)

            for i in range(0,len(indices)):

                parted_changed = changed[i].split(',')

                if len(indices[i]) == 1:

                    dataframe.loc[indices[i][0],'city'] = parted_changed[0]
                    dataframe.loc[indices[i][0],'state'] = parted_changed[1]

                else:
                    for j in range(0,len(indices[i])):
                        

                        dataframe.loc[indices[i][j],'city'] = parted_changed[0]
                        dataframe.loc[indices[i][j],'state'] = parted_changed[1]

            return dataframe
        
        except Exception as err:
            print(err)


    def filedownload( df, decide):

        """
            This function is useful for file downloading for Streamlit
            
            Args:

                df: This is the dataframe that comes up after preprocessing or clustering
                decide: This variable helps in establishing whether the call was from Provider or

            Returns:

                href: This is the downloadable link for the dataframe.
        """
        try:
            csv = df.to_csv(index=False)
            b64 = base64.b64encode(csv.encode()).decode()
            if decide == 1:
                href = f'<a href = "data:file/csv;base64,{b64}" download="Address.csv">Download CSV File</a>'
            else:
                href = f'<a href = "data:file/csv;base64,{b64}" download="Provider.csv">Download CSV File</a>'

            return href

        except Exception as err:
            print(err)


# ---------------------------------------------------------------------------------------------------
# Provider Functions

class Process_provider():

    def checkAutoCorrect(tokenlist):
        
        '''
        This function checks whether words are english or not and corrects the spelling mistakes
        Args : 

            tokenlist: list of token

        Returns: 
            
            temp.upper(): corrected string

        '''
        try:
            temp = ''
            spell = SpellChecker(language='en')
            dict = enchant.Dict("en_US")
            for token in tokenlist:
                # check for abbreviation
                if len(token) <= 3:
                    temp = temp+" "+token
                # check if word is english
                elif dict.check(token):
                    temp = temp+" "+token
                else:
                    # correct the word if spelling mistakes
                    correctword = str(spell.correction(
                        token)) if spell.correction(token) else token
                    temp = temp+" "+correctword

            original = ' '.join(tokenlist)
            print( f'original string:{original} , replaced string:{temp}, ratio: {fuzz.WRatio(original, temp)}')

            return temp.upper()
        
        except Exception as err:
            print(err)


    def update_provider_names(response, dataframe):
        
        '''
        This function updates the provider names based on the saved values in the grid table.

        Args : 

            response: This is the response from the grid-table that we are storing.
            dataframe: This is the dataframe provided by the user.

        Returns: 
            
            dataframe: This is the updated dataframe of the user after the corrections.

        '''
        try:

            original_not_to_change = []
            changed = []

            provider = list(dataframe['Provider_Name'])

            for index in response['selected_rows']:
                original_not_to_change.append(index['Provider_Name'])

            original = list(response['data']['Provider_Name'])
            changed = list(response['data']['Filtered_Name'])

            for part in range(0, len(original)):
                parted_original = original[part]
                parted_changed = changed[part]
                if original[part] in original_not_to_change:
                    continue
                elif parted_original in provider:
                    ind = provider.index(parted_original)
                    provider[ind] = parted_changed

            dataframe['Provider_Name'] = provider
            return dataframe
        
        except Exception as err:
            print(err)


    def duplicate_identifier(provider):

        """
        This function returns the duplicate provider name with respect to the original provider name.

        Args:

            provider: The provider names based on the dataframe provided
        
        Returns:

            duplicate: The clustered provider names based on the dataframe provided by the user.

        """
        try:
            neighbour_index_by_name = SortedNeighbourhood(on="Provider_Name", window=5)
            neighbour_index_by_name_pairs = neighbour_index_by_name.index(provider)
            print(f"total records {len(provider)} and no of paires {len(neighbour_index_by_name_pairs)}")
            compare_cl = rl.Compare()
            compare_cl.string("Provider_Name", "Provider_Name", label="provider_data_score")
            features = compare_cl.compute(neighbour_index_by_name_pairs, provider)
            print("comparison score", features)
            scores = np.average(features.values, axis=1)
            scored_comparison_vectors = features.assign(score=scores)
            scored_comparison_vectors.head(5)
            matches = features[features['provider_data_score'] >= 0.80]
            print("matched providers", matches)
            duplicate = list()
            for i in range(len(matches)):
                duplicate.append([provider.iloc[matches.index.get_level_values(0)[i], 1],
                                provider.iloc[matches.index.get_level_values(1)[i], 1]])

            duplicate = pd.DataFrame(duplicate,columns=['Provider Name','similar Provider Name'])
            return duplicate

        except Exception as err:
            print(err)
