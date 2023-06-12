import streamlit as st
import pandas as pd
from preprocessing import Preprocess
from grouping import Splink_linkage

from PIL import Image 
import json
import yaml


with open('path.json','r') as config:
    path = json.load(config)


# MAIN FUNCTION:
# ----------------------------------------------------

st.set_page_config(layout="wide")

def __main__():

    try:    
        
        st.sidebar.image(r'Dataset/Datasets/Citiustech_logo.png')

        st.write(""" # Record Linkage 2.0:
            We follow Febrl Dataset Standards. Please upload data according to Febrl Standards""")
        
        
        upload_file = st.sidebar.file_uploader("Please upload the configuration file:")
        
        if upload_file is not None:

            st.session_state['yaml_file'] = True
            loaded_config = yaml.safe_load(upload_file)

        if 'yaml_file' in st.session_state:
            
            dataset = loaded_config['dataset']
            operation = loaded_config['operation']
            dataset_type = loaded_config['dataset_type']
            geocoding = loaded_config['geocoding']

            if operation=='link_only':

                if dataset_type == 'mysql':

                    hostname = st.sidebar.text_input('Host Name:')
                    username = st.sidebar.text_input('Username:')
                    password = st.sidebar.text_input('Password:',type = 'password')
                    database = st.sidebar.text_input('Database Name:')
                    tables = st.sidebar.text_input('Table Names:')

                    if st.sidebar.button('Connect MySQL Database'):

                        process = Preprocess()
    
                        main_dataframeA, main_dataframeB = process.create_sql_dataframe( hostname, username, password, database, tables, operation)

                        st.session_state['show'] = True
                
                elif dataset_type=='sqlite':

                    database_file = st.sidebar.file_uploader("Upload the database file:",type = 'db')
                    tables = st.sidebar.text_input('Table Names:')

                    if st.sidebar.button('Submit Details'):

                        process = Preprocess()
                        main_dataframeA, main_dataframeB = process.create_dataframe( database_file, tables, operation)

                        st.session_state['show'] = True

                else:
                    uploaded_files = st.sidebar.file_uploader("Choose files to link together:",accept_multiple_files = True)

                    if st.sidebar.button('Upload Files'):

                        if len(uploaded_files)!=2:
                            st.write('Insufficient Number of files uploaded.')

                        if uploaded_files[0].name[-4::]=='xlsx' and uploaded_files[1].name[-4::]=='xlsx':

                            main_dataframeA = pd.read_excel(uploaded_files[0])
                            main_dataframeB = pd.read_excel(uploaded_files[1])

                        elif uploaded_files[0].name[-7::]=='parquet' and uploaded_files[1].name[-4::]=='parquet':
                            main_dataframeA = pd.read_parquet(uploaded_files[0])
                            main_dataframeB = pd.read_parquet(uploaded_files[1])

                        elif uploaded_files[0].name[-4::]=='json' and uploaded_files[1].name[-4::]=='json':
                            main_dataframeA = pd.read_json(uploaded_files[0])
                            main_dataframeB = pd.read_json(uploaded_files[1])

                        else:
                            main_dataframeA = pd.read_csv(uploaded_files[0])
                            main_dataframeB = pd.read_csv(uploaded_files[1])


                        st.session_state['show'] = True
                
                if 'show' in st.session_state:

                    if st.session_state['show']==True:
                        
                        process = Preprocess()
                        st.session_state['encryption'] = dataset
                        st.session_state['geocoding'] = geocoding

                        st.write(""" ##### First File: """)
                        dataframeA = main_dataframeA.copy()
                        st.dataframe( dataframeA, height = 500)

                        st.write(""" ##### Second File: """)
                        dataframeB= main_dataframeB.copy()
                        st.dataframe( dataframeB, height = 500)

                        if 'cluster' in main_dataframeA.columns and 'cluster' in main_dataframeB.columns:
                            st.session_state['label_exist'] = 1

                        else:
                            st.session_state['label_exist'] = 0

                        process.generate_key()

                        st.session_state['encrypted_dataframeA'] = process.Encrypter( main_dataframeA, st.session_state['encryption'])
                        st.session_state['encrypted_dataframeB'] = process.Encrypter( main_dataframeB, st.session_state['encryption'])

                        st.session_state['show'] = False


                if st.sidebar.button(' Preprocess ↻ '):

                    if 'preprocess' not in st.session_state:
                        
                        st.session_state['preprocess'] = True
                        st.session_state['splink'] = True
                
                if 'preprocess' in st.session_state:

                    if st.session_state['preprocess'] == True:
                        
                        process = Preprocess()

                        if st.session_state['geocoding']==True:
                            st.session_state['encrypted_dataframeA'] = process.geocoding( st.session_state['encrypted_dataframeA'], st.session_state['encryption'])

                        st.session_state['encrypted_dataframeA'] = process.record_linkage_preprocessing( st.session_state['encrypted_dataframeA'], st.session_state['encryption'])

                        st.session_state['encrypted_dataframeA'] = process.explore_dataframe( st.session_state['encrypted_dataframeA'], st.session_state['encryption'], operation)
                        
                        tab1,tab2 = st.tabs(['Preprocessed File','Dataset Information'])

                        with tab1:

                            st.write(""" ##### First File: """)

                            dataframeA = st.session_state['encrypted_dataframeA'].copy()
                            st.dataframe(process.Decrpyter(dataframeA,st.session_state['encryption']))

                        with tab2:

                            col1,col2 = st.columns(2)
                            if st.session_state['encryption']=='febrl':
                                with col1:
                                    
                                    st.image(path['base_febrl_path']+path['missingness'],width=450)
                                    st.image(path['base_febrl_path']+path['columns_profile'],width = 630)

                            else:
                                with col1:

                                    st.image(path['base_custom_path']+path['missingness'],width = 450)
                                    st.image(path['base_custom_path']+path['columns_profile'], width = 630)
                        
                        if st.session_state['geocoding']==True:
                            st.session_state['encrypted_dataframeB'] = process.geocoding( st.session_state['encrypted_dataframeB'], st.session_state['encryption'])

                        st.session_state['encrypted_dataframeB'] = process.record_linkage_preprocessing( st.session_state['encrypted_dataframeB'], st.session_state['encryption'])
                        st.session_state['encrypted_dataframeB'] = process.explore_dataframe( st.session_state['encrypted_dataframeB'], st.session_state['encryption'], operation)

                        with tab1:

                            st.write(""" ##### Second File: """)

                            dataframeB = st.session_state['encrypted_dataframeB'].copy()
                            st.dataframe(process.Decrpyter(dataframeB,st.session_state['encryption']))

                        with tab2:

                            if st.session_state['encryption']=='febrl':
                                with col2:

                                    st.image(path['base_febrl_path']+path['missingness'],width=450)
                                    st.image(path['base_febrl_path']+path['columns_profile'],width = 630)

                            else:
                                with col2:

                                    st.image(path['base_custom_path']+path['missingness'],width = 450)
                                    st.image(path['base_custom_path']+path['columns_profile'], width = 630)

                        st.session_state['preprocess'] = False


                if 'splink' in st.session_state:

                    if st.sidebar.button('Record Linkage'):
                        
                        splink_method = Splink_linkage()
                        process = Preprocess()

                        splink_dataframe = splink_method.Linking( st.session_state['encrypted_dataframeA'], st.session_state['encrypted_dataframeB'], st.session_state['label_exist'], st.session_state['encryption'])

                        tab1,tab2 = st.tabs(['Model Output','Model Performance'])
                        
                        with tab1:

                            st.dataframe(splink_dataframe)

                        with tab2:

                            if st.session_state['encryption']=='febrl':

                                col1, col2 = st.columns(2)
                                with col1:

                                    st.image(path['base_febrl_linkage']+path['mu_parameters'],width=600)
                                    print("Helooooooooooooooooooooooooooooooooooooooooooooo")
                                    st.image(path['base_febrl_linkage']+path['match_weight'],width=600)

                                with col2:

                                    if st.session_state['label_exist']==1:
                                        st.image(path['base_febrl_linkage']+path['precision_recall'],width=500)
                                        st.image(path['base_febrl_linkage']+path['roc_auc_chart'],width=500)
                            else:
                                col1, col2 = st.columns(2)
                                with col1:

                                    st.image(path['base_custom_linkage']+path['mu_parameters'],width=600)
                                    st.image(path['base_custom_linkage']+path['match_weight'],width=600)

                                with col2:

                                    if st.session_state['label_exist']==1:
                                        st.image(path['base_custom_linkage']+path['precision_recall'],width=500)
                                        st.image(path['base_custom_linkage']+path['roc_auc_chart'],width=500)
                    


            
            elif operation=='dedupe_only':

                if dataset_type == 'mysql':

                    hostname = st.sidebar.text_input('Host Name:')
                    username = st.sidebar.text_input('Username:')
                    password = st.sidebar.text_input('Password:',type = 'password')
                    database = st.sidebar.text_input('Database Name:')
                    tables = st.sidebar.text_input('Table Names:')

                    if st.sidebar.button('Connect MySQL Database'):
                        process = Preprocess()
                        main_dataframe = process.create_sql_dataframe( hostname, username, password, database, tables, operation)

                        st.session_state['show_dedupe'] = True

                elif dataset_type=='sqlite':

                    database_file = st.sidebar.file_uploader("Upload the database file:",type = 'db')
                    tables = st.sidebar.text_input('Table Names:')

                    if st.sidebar.button('Submit Details'):

                        process = Preprocess()

                        main_dataframe = process.create_dataframe( database_file, tables, operation)

                        st.session_state['show_dedupe'] = True
                
                else:

                    uploaded_file = st.sidebar.file_uploader("Choose a file to deduplicate:")

                    if uploaded_file is not None:

                        if 'load_file' not in st.session_state:
                
                            if uploaded_file.name[-4::]=='xlsx':

                                main_dataframe = pd.read_excel(uploaded_file)
                            
                            elif uploaded_file.name[-7::] == 'parquet':

                                main_dataframe = pd.read_parquet(uploaded_file)
                            
                            elif uploaded_file.name[-4::] == 'json':

                                main_dataframe = pd.read_json(uploaded_file)
                                
                            else:

                                main_dataframe = pd.read_csv(uploaded_file)
                            
                            st.session_state['load_file'] = True
                            st.session_state['show_dedupe'] = True

                if 'show_dedupe' in st.session_state:

                    if st.session_state['show_dedupe'] == True:

                        process = Preprocess()

                        st.write(""" ##### Original File Preview: """)
                        dataframeC = main_dataframe.copy()
                        st.dataframe(dataframeC, height=550)

                        st.session_state['encryption'] = dataset
                        st.session_state['geocoding'] = geocoding

                        input_dataframe = main_dataframe

                        if 'cluster' in input_dataframe.columns:
                            st.session_state['label_exist'] = 1

                        else:
                            st.session_state['label_exist'] = 0
                            
                        process.generate_key()

                        st.session_state['encrypted_dataframe'] = process.Encrypter( input_dataframe, st.session_state['encryption'])
                        st.session_state['show_dedupe'] = False

                if st.sidebar.button(' Preprocess ↻ '):

                    if 'preprocess_duplication' not in st.session_state:
                        
                        st.session_state['preprocess_duplication'] = True
                        st.session_state['splink_duplication'] = True
                
                if 'preprocess_duplication' in st.session_state:

                    if st.session_state['preprocess_duplication'] == True:
                        
                        process = Preprocess()
                        if st.session_state['geocoding']==True:
                            st.session_state['encrypted_dataframe'] = process.geocoding( st.session_state['encrypted_dataframe'], st.session_state['encryption'])

                        st.session_state['encrypted_dataframe'] = process.record_linkage_preprocessing( st.session_state['encrypted_dataframe'], st.session_state['encryption'])
                        
                        st.session_state['encrypted_dataframe'] = process.explore_dataframe( st.session_state['encrypted_dataframe'], st.session_state['encryption'], operation)
                        
                        tab1,tab2 = st.tabs(['Preprocessed File','Dataset Information'])

                        with tab1:
                    
                            dataframeA = st.session_state['encrypted_dataframe'].copy()
                            st.dataframe(process.Decrpyter(dataframeA,st.session_state['encryption']))

                        with tab2:
                            if st.session_state['encryption']=='febrl':
                                    
                                st.write(r'Missingness values with respect to dataset:')
                                st.image(path['base_febrl_path']+path['missingness'],width=450)
                                st.write(r'Information about the columns:')
                                st.image(path['base_febrl_path']+path['columns_profile'],width=630)

                            else:

                                st.write(r'Missingness values with respect to dataset:')
                                st.image(path['base_custom_path']+path['missingness'],width=450)
                                st.write(r'Information about the columns:')
                                st.image(path['base_custom_path']+path['columns_profile'],width=630)


                        st.session_state['preprocess_duplication'] = False


            
                    if 'splink_duplication' in st.session_state:

                        if st.sidebar.button('Record Linkage'):

                            splink_method = Splink_linkage()
                            process = Preprocess()
                            splink_dataframe = splink_method.Deduplication( st.session_state['encrypted_dataframe'], st.session_state['label_exist'], st.session_state['encryption'])
                        
                            tab1,tab2 = st.tabs(['Model Output','Model Performance'])
                            
                            with tab1:
                                st.dataframe(splink_dataframe)

                            with tab2:
                                if st.session_state['encryption']=='febrl':
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.image(path['base_febrl_deduplication']+path['mu_parameters'],width=600)
                                        st.image(path['base_febrl_deduplication']+path['match_weight'],width=600)
                                    with col2:
                                        if st.session_state['label_exist']==1:
                                            st.image(path['base_febrl_linkage']+path['precision_recall'],width=500)
                                            st.image(path['base_febrl_linkage']+path['roc_auc_chart'],width=500)
                                else:
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.image(path['base_custom_deduplication']+path['mu_parameters'],width=600)
                                        st.image(path['base_custom_deduplication']+path['match_weight'],width=600)
                                    with col2:
                                        if st.session_state['label_exist']==1:
                                            st.image(path['base_custom_deduplication']+path['precision_recall'],width=500)
                                            st.image(path['base_custom_deduplication']+path['roc_auc_chart'],width=500)
                        



    except Exception as err:
        print(err)
        st.experimental_rerun()

__main__()