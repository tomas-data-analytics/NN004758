import streamlit as st
import pandas as pd
import plotly.express as px
from NN004758_ETL import run_ETL
from io import BytesIO



st.set_page_config(layout="wide")

#test changes git

def main_page():
    st.title('Novo Nordisk US 4758 - Autoclean')
    st.write('Just for internal use - January 2025')

def load_EDC():
    st.title('EDC')
    EDC_raw = st.file_uploader('Select EDC file in .csv format', type="csv")
    
    if EDC_raw is not None:
        st.session_state['EDC_raw'] = EDC_raw
        try:
            if EDC_raw.size > 0:
                st.write("File uploaded successfully.")
                EDC_raw.seek(0)  # Reset the file pointer to the beginning
                EDC_raw = pd.read_csv(EDC_raw, header=None)
                st.write(EDC_raw)
                st.session_state['EDC_df'] = EDC_raw
            else:
                st.error("The uploaded file is empty.")
        except pd.errors.EmptyDataError:
            st.error("The uploaded file is empty or has no columns to parse.")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    elif 'EDC_raw' in st.session_state:
        try:
            st.session_state['EDC_raw'].seek(0)  # Reset the file pointer to the beginning
            EDC_raw = pd.read_csv(st.session_state['EDC_raw'], header=None)
            st.write(EDC_raw)
            st.session_state['EDC_df'] = EDC_raw
        except pd.errors.EmptyDataError:
            st.error("The uploaded file is empty or has no columns to parse.")
        except Exception as e:
            st.error(f"An error occurred: {e}")

def load_VBT():
    st.title('Cleaning Notes')
    VBT = st.file_uploader('Select Cleaning notes from browser', type="xlsx")
    
    if VBT is not None:
        st.session_state['VBT'] = VBT
        try:
            # Leer las diferentes hojas del archivo Excel
            VBT_df = pd.read_excel(VBT, sheet_name='Visit Chart')
            transfer_subjects = pd.read_excel(VBT, sheet_name='Transfer Subjects')
            delete_visits = pd.read_excel(VBT, sheet_name='Delete Visits')
            include_visits = pd.read_excel(VBT, sheet_name='Withdrawn RY Payable')
            
            st.write('Cleaning notes uploaded')
            st.write('Visit Chart:', VBT_df)
            st.write('Transfer Subjects:', transfer_subjects)
            st.write('Delete Visits:', delete_visits)
            st.write('Withdrawn RY Payable:', include_visits)
            
            # Almacenar DataFrames en session state
            st.session_state['VBT_df'] = VBT_df
            st.session_state['transfer_subjects'] = transfer_subjects
            st.session_state['delete_visits'] = delete_visits
            st.session_state['include_visits'] = include_visits
        except FileNotFoundError:
            st.error(f"File not found.")
        except Exception as e:
            st.error(f"Error reading the file: {e}")
    elif 'VBT' in st.session_state:
        try:
            # Leer las diferentes hojas del archivo Excel almacenado en la sesión
            VBT = st.session_state['VBT']
            VBT_df = pd.read_excel(VBT, sheet_name='Visit Chart')
            transfer_subjects = pd.read_excel(VBT, sheet_name='Transfer Subjects')
            delete_visits = pd.read_excel(VBT, sheet_name='Delete Visits')
            include_visits = pd.read_excel(VBT, sheet_name='Withdrawn RY Payable')
            
            st.write('Cleaning notes uploaded')
            st.write('Visit Chart:', VBT_df)
            st.write('Transfer Subjects:', transfer_subjects)
            st.write('Delete Visits:', delete_visits)
            st.write('Withdrawn RY Payable:', include_visits)
            
            # Almacenar DataFrames en session state
            st.session_state['VBT_df'] = VBT_df
            st.session_state['transfer_subjects'] = transfer_subjects
            st.session_state['delete_visits'] = delete_visits
            st.session_state['include_visits'] = include_visits
        except FileNotFoundError:
            st.error(f"File not found.")
        except Exception as e:
            st.error(f"Error reading the file: {e}")

def load_ER():
    st.title('Echo report - sent by email')
    echo_report = st.file_uploader('Select Echo Report from browser', type="xlsx")
    
    if echo_report is not None:
        st.session_state['echo_report'] = echo_report
        try:
            echo_report = pd.read_excel(echo_report, sheet_name='Study Echos')
            st.session_state['echo_report_df'] = echo_report
            st.write(echo_report)
        except Exception as e:
            st.error(f"Error reading the file: {e}")
    elif 'echo_report' in st.session_state:
        if isinstance(st.session_state['echo_report'], pd.DataFrame):
            echo_report = st.session_state['echo_report']
            st.write(st.session_state['echo_report_df'])
            st.session_state['echo_report_df'] = echo_report
        else:
            try:
                echo_report = pd.read_excel(st.session_state['echo_report'], sheet_name='Study Echos')
                st.write(echo_report)
                st.session_state['echo_report_df'] = echo_report
            except Exception as e:
                st.error(f"Error reading the file: {e}")

def load_CP():
        if 'EDC_df' in st.session_state and 'VBT_df' in st.session_state and 'transfer_subjects' in st.session_state and 'delete_visits' in st.session_state and 'include_visits' in st.session_state and 'echo_report_df' in st.session_state:
            cleaned_data = run_ETL(st.session_state['EDC_df'], st.session_state['VBT_df'], st.session_state['transfer_subjects'], st.session_state['delete_visits'], st.session_state['include_visits'], st.session_state['echo_report_df'])
            st.write("Cleaned Data after ETL:", cleaned_data)
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                cleaned_data.to_excel(writer, index=False, sheet_name='clean_file')
            output.seek(0)  # Volver al inicio del archivo en memoria

            st.download_button(
                label="Download Clean File",
                data=output.getvalue(),
                file_name='NN004758_clean.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            st.error("Please load all required files first.")
            return None

# Sidebar navigation
st.sidebar.title('Navigation')
page = st.sidebar.selectbox('Select Task', ['Load EDC file', 'Load Cleaning notes', 'Load Echo Report', 'Start cleaning process',])

main_page()

if page == 'Load EDC file':
    load_EDC()
elif page == 'Load Cleaning notes':
    load_VBT()
elif page == 'Load Echo Report':
    load_ER()
elif page == 'Start cleaning process':
    if st.button('Start Cleaning Process'):
        load_CP()