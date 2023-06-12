import streamlit as st
import pandas as pd
import enchant
from nltk import word_tokenize
from sentence_transformers import SentenceTransformer
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from peprocessing import Preprocess, Process_provider
from clustering import Clustering
import time

predefined_dataset_path = 'Dataset/cities_states.csv'
def model_declare():
    """

        Declaring & loading the saved model stored locally for calculating the similarity percentage

    """
    try:

        path = 'local/path/to/model'
        model = SentenceTransformer(path)

        return model

    except Exception as err:

        print(err)


# MAIN FUNCTION:
# ----------------------------------------------------

def __main__():

    try:
        st.set_page_config(layout="wide")
        
        with st.sidebar:
            st.markdown(""" __Select a Data Attribute from list below__ """)
            page = st.selectbox('Select:', ['Address', 'Provider'])

        if page == 'Address':
            
            
            # Choosing Address in the drop down will execute the further code

            st.write(""" # Address Data Quality:
                Please upload the required dataset.""")

            uploaded_file = st.sidebar.file_uploader("Choose a file")

            if uploaded_file is not None:

                if 'input_dataframe' not in st.session_state:
                    print(uploaded_file.name)
                    if uploaded_file.name[-4::]=='xlsx':

                        st.session_state['input_dataframe'] = pd.read_excel(uploaded_file)
                    
                    elif uploaded_file.name[-7::] == 'parquet':
                        st.session_state['input_dataframe'] = pd.read_parquet(uploaded_file)
                    
                    elif uploaded_file.name[-4::] == 'json':
                        st.session_state['input_dataframe'] = pd.read_json(uploaded_file)
                        
                    else:   
                        st.session_state['input_dataframe'] = pd.read_csv(uploaded_file)

                    input_dataframe = st.session_state['input_dataframe']
                    correct_column_names = [ 'address', 'city', 'state', 'zip']
                    column_names = list(input_dataframe.columns)

                    if len(column_names)!=4:

                        st.error('The data is not in a proper format', icon = "🚨")
                        
                        for key in st.session_state.keys():
                            del st.session_state[key]

                        st.experimental_rerun()

                    for column in correct_column_names:

                        if column in column_names:
                            continue

                        else:
                            st.error('The data is not in a proper format', icon = "🚨")
                        
                            for key in st.session_state.keys():
                                del st.session_state[key]

                            st.experimental_rerun()
                    
                    st.write(""" ##### File Preview:""")
                    st.dataframe(st.session_state['input_dataframe'], width=650, height=550)

                if st.sidebar.button(' Preprocess ↻ '):

                    if 'preprocess' not in st.session_state:

                        st.session_state['preprocess'] = True
                        st.session_state['abbreviation'] = True
                        st.session_state['show_abbreviation'] = True

            if 'show_abbreviation' in st.session_state:

                if st.sidebar.button('Generate Complete File..'):

                
                    # This button will correct all the common abbreviations present in the file such as "Rd, Hwy"

                    if st.session_state['abbreviation'] == True:
                        
                        data_frame = Preprocess.update_values(st.session_state['response'], st.session_state['dataframe'])
                        tokens = Preprocess.preprocess_abbreviation(data_frame)
                        st.session_state['dataframe']['address'] = tokens
                        st.session_state['dataframe'] = data_frame
                        st.session_state['abbreviation'] = False
                        st.session_state['show_cluster'] = True
                        st.session_state['model'] = model_declare()

                    st.session_state['preprocess'] = False
                    st.markdown(Preprocess.filedownload(st.session_state['dataframe'], decide=1), unsafe_allow_html=True)
                    st.write('#### Complete Preprocessed File: ')
                    st.dataframe(st.session_state['dataframe'], width=650, height=550)

            if 'show_cluster' in st.session_state:
                
                if st.sidebar.button('Proceed for Clustering'):

                        # This is the code for the clustering algorithm, which checks the similarity score, further
                        # processes to checking of the numbers in the code and if similar, groups them together and forms
                        # a cluster.   

                    Cluster = {}
                    length = 1
                    threshold = 0.75

                    dataframe = st.session_state['dataframe']
                    Address = Clustering.convert_to_single(dataframe['address'], dataframe['city'], dataframe['state'], dataframe['zip'])
                    st.session_state['Cluster_Assign'] = [0 for i in range(0,len(dataframe['address']))]
                    print(Address)
                    for main_address in range(0, len(Address)):
                        
                        for loop_address in range(0, len(Address)):
                            
                            if main_address == loop_address:

                                continue

                            else:

                                main_string, second_string = Clustering.Digit_Removal(Address[main_address], Address[loop_address])
                                print(main_string, second_string)
                                main_embeddings = st.session_state['model'].encode([main_string])[0]
                                second_embeddings = st.session_state['model'].encode([second_string])[0]
                                similarity_percentage = Clustering.cosine_function(main_embeddings, second_embeddings)
                                # print(similarity_percentage)
                                if similarity_percentage >= threshold:
                                    check = Clustering.Digit_Comparision(Address[main_address], Address[loop_address])
                                    
                                    if check:
                                        Cluster, length, st.session_state['Cluster_Assign'] = Clustering.Cluster_Formation( Address[main_address], Address[loop_address], Cluster, length, st.session_state['Cluster_Assign'], main_address, loop_address)
                    
                    dataframe['Groups'] = st.session_state['Cluster_Assign']
                    dataframe.to_csv('Result.csv')
                    # dataframe = pd.read_csv('Dataset/Result.csv')
                    # st.markdown(Preprocess.filedownload(dataframe, decide=1),unsafe_allow_html=True)
                    # st.dataframe(dataframe, width=650, height=550)

            if 'preprocess' in st.session_state:

                if st.session_state['preprocess'] == True:

                    
                    # This is the code for spell-correction and pre-processing which will correct the values of 
                    # the data-attribute respectively.

                    df = st.session_state['input_dataframe']
                    st.session_state['dataframe'] = df
                    predefined_dataset = pd.read_csv(predefined_dataset_path)

                    city_state_sample, city_state_predefined = Preprocess.initialize(df, predefined_dataset)
                    st.session_state['corrected_dataframe'] = Preprocess.correct_change(df, predefined_dataset, city_state_predefined)
                    
                    st.write("## Corrected States & Cities: ")

                    
                    # This will show the grid-table having the original values and also the correct values
                    # which are completely editable.

                    gd = GridOptionsBuilder.from_dataframe(st.session_state['corrected_dataframe'])
                    gd.configure_default_column(editable=True, groupable=True)
                    gd.configure_selection(selection_mode="multiple", use_checkbox=True)
                    gridoptions = gd.build()
                    response = AgGrid(st.session_state['corrected_dataframe'], gridOptions=gridoptions, editable=True, theme='balham', update_mode=GridUpdateMode.MANUAL, allow_unsafe_jscode=True, height=200, fit_columns_on_grid_load=True)

                    st.markdown("""                 *Note:* 
                        
                        - Don't forget to hit enter ↩ on to update.
                        - If you want the original value to be retained ✓ the check box. """)

                    st.session_state['response'] = response


        else:

            st.write(""" # Provider Data Quality:
            Please upload the required dataset. """)

            uploaded_file = st.sidebar.file_uploader("Choose a Excel or CSV file", type=["csv", "xslx"])

            if uploaded_file is not None:

                if 'input_df' not in st.session_state:

                    dataset1 = pd.read_csv(uploaded_file)
                    st.session_state['input_df'] = dataset1
                    st.write("File Preview:")
                    st.dataframe(dataset1, width=750, height=450)

                if st.sidebar.button(' Preprocess ↻ '):

                    if 'preprocess_provider' not in st.session_state:
                        st.session_state['preprocess_provider'] = True
                        st.session_state['complete_file'] = True
                        st.session_state['final'] = True

            if 'final' in st.session_state:

                if st.sidebar.button('Generate Preprocessed File.'):
                    if st.session_state['complete_file'] == True:

                        data_frame = Process_provider.update_provider_names(st.session_state['response'], st.session_state['dataframe'])

                    st.session_state['preprocess_provider'] = False
                    final_dataframe = st.session_state['input_df']
                    final_dataframe['Provider_Name'] = data_frame['Provider_Name']
                    st.markdown( Preprocess.filedownload(final_dataframe, decide=0), unsafe_allow_html=True)

                    st.write('#### Complete Preprocessed File: ')
                    st.dataframe(final_dataframe, width=750, height=450)

                    # duplicate_records= Process_provider.duplicate_identifier(st.session_state['input_df'])

                    # st.markdown( Preprocess.filedownload(final_dataframe, decide=0), unsafe_allow_html=True)
                    # st.write('#### Duplicate Records: ')
                    # st.dataframe(duplicate_records, width=650, height=550)
                    



            if 'preprocess_provider' in st.session_state:

                if st.session_state['preprocess_provider'] == True:

                    dict = enchant.Dict("en_US")

                    dataset1 = pd.DataFrame(st.session_state['input_df']["Provider_Name"], columns=["Provider_Name"])
                    # tokenization of provider name
                    dataset1["tokenized_Name"] = [list(word_tokenize(x)) for x in dataset1["Provider_Name"]]
                    # correction  string
                    dataset1["Filtered_Name"] = [Process_provider.checkAutoCorrect(x) for x in dataset1["tokenized_Name"]]

                    st.write("## Corrected Provider Names: ")

                    st.session_state['dataframe'] = dataset1
                    gd = GridOptionsBuilder.from_dataframe(dataset1[["Provider_Name", "Filtered_Name"]])
                    gd.configure_default_column(editable=True, groupable=True)
                    gd.configure_selection(selection_mode="multiple", use_checkbox=True)
                    gridoptions = gd.build()
                    response = AgGrid(dataset1[["Provider_Name", "Filtered_Name"]],
                                    gridOptions=gridoptions,
                                    editable=True,
                                    theme='balham',
                                    update_mode=GridUpdateMode.MANUAL,
                                    allow_unsafe_jscode=True,
                                    height=200,
                                    fit_columns_on_grid_load=True)

                    st.markdown("""                 *Note:* 
                        
                        - Don't forget to hit enter ↩ on to update.
                        - If you want the original value to be retained ✓ the check box. """)

                    sel_rows = response['selected_rows']
                    st.session_state['response'] = response


    except Exception as err:
        print(err)
        st.experimental_rerun()

__main__()