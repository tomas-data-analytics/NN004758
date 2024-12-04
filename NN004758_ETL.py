import pandas as pd
import numpy as np
import glob
import streamlit as st
from datetime import datetime



def run_ETL(EDC_raw, VBT_df, transfer_subjects, delete_visits, include_visits, echo_report):
    
    clean_file = EDC_raw
    # delete all empty columns
    clean_file = clean_file.dropna(axis=1, how='all')
    clean_file.insert(5, 'r','')
    clean_file = clean_file.drop([0, 1]).reset_index(drop=True)
    clean_file.reset_index(drop=True, inplace=True)
    clean_file = clean_file.iloc[:, 2:]
    clean_file.columns = ['Protocol ID', 'Site', 'Subject', 'Randomization', 'Visit', 'Visit Date', 'Status', 'Visit_Type', 'SAE Symptoms']
    clean_file = clean_file.drop(index=0).reset_index(drop=True)
    #clean_file = clean_file.iloc[1:, :]

    clean_file['Protocol ID'] = 'NN004758'

    SAE = clean_file[clean_file['Visit'] == 'SAE']

    #delete 'SAE' visits from clean
    clean_file = clean_file[~clean_file['Visit'].str[:3].str.contains('SAE')]
    clean_file = clean_file.drop(columns=['SAE Symptoms'])

    #delete empty 'visit date' rows
    missed_dates = clean_file[clean_file['Visit Date'].isna()]
    clean_file = clean_file.dropna(subset=['Visit Date'])

    #check visit types and delete 'visit missed' rows
    clean_file = clean_file[clean_file['Visit_Type'] != 'Visit missed']

    #convert 'Site' and 'Subject' to number
    clean_file['Site'] = pd.to_numeric(clean_file['Site'], errors='coerce')
    clean_file['Subject'] = pd.to_numeric(clean_file['Subject'], errors='coerce')

    # Convert dates
    
    clean_file['Visit Date'] = pd.to_datetime(clean_file['Visit Date'], format='%Y%m%d').dt.strftime('%m/%d/%Y')

    #sort
    clean_file = clean_file.sort_values(by=['Site', 'Subject', 'Visit Date'], ascending=[True, True, True])

    # Create new data frame with Visit 1 (screening visits) with 'Randomized' status:
    retention_year_raw = clean_file[(clean_file['Visit'] == 'Visit 1') & (clean_file['Status'] == 'Randomized')]

    # Convert date format
    retention_year_raw['Visit Date'] = pd.to_datetime(retention_year_raw['Visit Date'], format='%m/%d/%Y')

    # difference between years 
    today = datetime.today()

    # Creating a list to save new rows
    new_rows = []

    for index, row in retention_year_raw.iterrows():
        visit_date = row['Visit Date']
        years_since_visit = int((today - visit_date).days / 365.25)
        
        # creating new rows for each year
        for year in range(1, years_since_visit + 1):
            new_row = row.copy()
            new_row['Visit'] = f'Retention Year {year}'
            new_row['Visit Date'] = (visit_date + pd.DateOffset(years=year)).strftime('%m/%d/%Y')
            new_rows.append(new_row)

    # creating a new data frame
    ret_visit = pd.DataFrame(new_rows)

    # concatenate both data frames
    ret_visit = pd.concat([retention_year_raw, ret_visit], ignore_index=True)

    # Convert date format
    ret_visit['Visit Date'] = pd.to_datetime(ret_visit['Visit Date'], errors='coerce')
    ret_visit['Visit Date'] = ret_visit['Visit Date'].dt.strftime('%m/%d/%Y')

    #sort
    ret_visit = ret_visit.sort_values(by=['Site', 'Subject', 'Visit Date'], ascending=[True, True, True])

    #back to our clean file
    clean_file = pd.concat([clean_file, ret_visit], axis=0, ignore_index=True)

    # Convertir la columna 'Visit Date' a formato datetime
    clean_file['Visit Date'] = pd.to_datetime(clean_file['Visit Date'], errors='coerce')

    # Convertir la columna 'Visit Date' al formato deseado 'MM/DD/YYYY'
    clean_file['Visit Date'] = clean_file['Visit Date'].dt.strftime('%m/%d/%Y')

    #drop duplicates
    clean_file = clean_file.drop_duplicates(subset=clean_file.columns.difference(['Visit Date']))

    # Convert the 'Visit Date' column to datetime format to ensure correct ordering
    clean_file['Visit Date'] = pd.to_datetime(clean_file['Visit Date'], format='%m/%d/%Y')

    # Sort the DataFrame by 'Site', 'Subject', and 'Visit Date' columns in ascending order
    clean_file = clean_file.sort_values(by=['Site', 'Subject', 'Visit Date'], ascending=[True, True, True])

    #echo report

    #delete empty columns
    echo_report = echo_report.dropna(axis=1, how='all')
    echo_report.insert(3, 'NewColumn', '')
    echo_report.columns = ['Protocol ID', 'Site', 'Subject_raw', 'Randomization', 'Visit Name', 'Visit Date', 'Media','QA', 'Notes']
    echo_report.insert(3, 'Subject', echo_report['Site'].astype(str) + echo_report['Subject_raw'].astype(str))

    echo_report = echo_report.drop(columns=['Subject_raw','Media','QA', 'Notes'])

    echo_report = echo_report[echo_report['Protocol ID'] == 'United States']
    echo_report['Protocol ID'] = 'NN004758'

    echo_report['Subject Status'] = ''

    echo_report['Visit Name'] = echo_report['Visit Name'].replace({'Baseline': 'V2 Echo', 'V12': 'V12 Echo'})
    echo_report['Subject Status'] = ''

    echo_report['Subject'] = pd.to_numeric(echo_report['Subject'])
    clean_file['Subject'] = pd.to_numeric(clean_file['Subject'])

    echo_report = echo_report.merge(clean_file[['Subject', 'Status']], on='Subject', how='left', suffixes=('', '_clean'))

    echo_report = echo_report.drop(columns=['Subject Status'])
    echo_report = echo_report.rename(columns={'Visit Name': 'Visit'})

    echo_report = echo_report.drop_duplicates()

    #back to clean file 

    # Convert the 'Visit Date' column to datetime format to ensure correct ordering
    clean_file['Visit Date'] = pd.to_datetime(clean_file['Visit Date'], errors='coerce')

    # Sort the DataFrame by 'Site', 'Subject', and 'Visit Date' columns in ascending order
    clean_file = clean_file.sort_values(by=['Site', 'Subject', 'Visit Date'], ascending=[True, True, True])

    # Create a list to store the indices of rows to be removed
    rows_to_remove = []

    # Iterate over each unique subject
    for subject in clean_file['Subject'].unique():
        # Filter the DataFrame by the current subject
        subject_df = clean_file[clean_file['Subject'] == subject]
        
        # Iterate over the rows of the filtered DataFrame
        for i in range(len(subject_df)):
            current_visit = subject_df.iloc[i]
            
            # Check if the current visit starts with 'Retention '
            if current_visit['Visit'].startswith('Retention Year '):
                # Check if it is the last visit for the subject
                if i == len(subject_df) - 1:
                    rows_to_remove.append(current_visit.name)
                # Check if the next visit also starts with 'Retention '
                elif i < len(subject_df) - 1 and subject_df.iloc[i + 1]['Visit'].startswith('Retention Year '):
                    rows_to_remove.append(current_visit.name)
                    rows_to_remove.append(subject_df.iloc[i + 1].name)

    # Remove the identified rows from the DataFrame
    clean_file = clean_file.drop(rows_to_remove)

    clean_file.loc[clean_file['Visit_Type'] == 'Telephone contact', 'Visit'] = 'Phone ' + clean_file['Visit']

    clean_file = clean_file.drop(columns=['Visit_Type'])

    clean_file = pd.merge(clean_file, VBT_df, on='Visit', how='left')

    columns = list(clean_file.columns)
    visit_index = columns.index('Visit')
    columns.insert(visit_index + 1, columns.pop(columns.index('InSite Name')))

    clean_file = clean_file[columns]

    # Eliminar la columna "Visit"
    clean_file = clean_file.drop(columns=['Visit'])
    clean_file = clean_file.rename(columns={'InSite Name': 'Visit'})

    clean_file = pd.concat([clean_file, echo_report], ignore_index=True)
    clean_file['Visit Date'] = pd.to_datetime(clean_file['Visit Date']).dt.strftime('%m/%d/%Y')
    clean_file['Visit Date'] = pd.to_datetime(clean_file['Visit Date'])

    clean_file = clean_file.sort_values(by='Visit Date')

    clean_file['Status'] = clean_file['Status'].replace({'SF': 'Screen Fail', 'VWithdrawn': 'Discontinued'})

    #transferred subjects
    #delete empty columns
    transfer_subjects = transfer_subjects.dropna(axis=1, how='all')
    transfer_subjects['To Site'] = pd.to_numeric(transfer_subjects['To Site'], errors='coerce').fillna(0).astype(int)
    transfer_subjects['From Site'] = pd.to_numeric(transfer_subjects['From Site'], errors='coerce').fillna(0).astype(int)
    transfer_subjects['Original Site'] = pd.to_numeric(transfer_subjects['Original Site'], errors='coerce').fillna(0).astype(int)
    transfer_subjects['Effective Date'].fillna(pd.Timestamp(datetime.today().strftime('%Y-%m-%d')), inplace=True)
    #transfer_subjects['Subject'] = pd.to_numeric(transfer_subjects['Subject'], errors='coerce').fillna(0).astype(int)

    clean_file['Visit Date'] = pd.to_datetime(clean_file['Visit Date'], errors='coerce')
    transfer_subjects['Effective Date'] = pd.to_datetime(transfer_subjects['Effective Date'], errors='coerce')

    # Iterate rows in transfer_subjects
    for index, row in transfer_subjects.iterrows():
        subject = row['Subject']
        effective_date = row['Effective Date']
        to_site = row['To Site']
        from_site = row['From Site']
        
        # Crear la máscara para seleccionar las filas correctas
        mask = (clean_file['Subject'] == subject) & (clean_file['Visit Date'] >= effective_date)
        
        # Actualizar la columna 'Site' en clean_file si la fecha de transferencia es mayor o igual
        clean_file.loc[mask, 'Site'] = to_site
        
        # Crear la máscara para las filas que no cumplen la condición
        mask_not_met = (clean_file['Subject'] == subject) & (clean_file['Visit Date'] < effective_date)
        
        # Actualizar la columna 'Site' en clean_file si la fecha de transferencia no es mayor o igual
        clean_file.loc[mask_not_met, 'Site'] = from_site
        

    #delete non US sites 145, 150 & 240
    clean_file = clean_file[~clean_file['Site'].isin([145, 150, 240])]

    # Remove rows where 'Site' is 144, 184, or 187 and 'Visit' is 'V2 Echo'
    clean_file = clean_file[~((clean_file['Site'].isin([144, 184, 187])) & (clean_file['Visit'] == 'V2 Echo'))]

    #for site 271

    def extract_first_three_numbers(subject):
        numbers = ''.join(filter(str.isdigit, str(subject)))[:3]
        return int(numbers) if numbers else 0

    clean_file.loc[clean_file['Site'] == 0, 'Site'] = clean_file.loc[clean_file['Site'] == 0, 'Subject'].apply(extract_first_three_numbers)

    #Delete visits from 'Delete Visits' TAB in cleaning notes

    # Iterate over each row in delete_visits
    for index, row in delete_visits.iterrows():
        subject = row['Subject']
        visit = row['Visit']
        
        # Create a mask to select the rows to be deleted
        mask = (clean_file['Subject'] == subject) & (clean_file['Visit'] == visit)
        
        # Delete the rows that match the condition
        clean_file = clean_file[~mask]

    #update statuses

    clean_file['Status'] = clean_file['Status'].replace({'Enrolled': 'Randomized', 'Withdrawn': 'Discontinued', 'Started': 'Randomized','Randomized': 'Randomized', 'End of\nTreatment': 'Complete','End of Treatment': 'Complete', 'Screen Failed': 'Screen Fail','Screen Failure': 'Screen Fail','Screened':'Randomized', 'In Screening':'Screening','Started\nTreatment':'Randomized'})

    #Manual adjustments made after Lead confirmation 

    clean_file.loc[clean_file['Subject'] == 137007, 'Status'] = 'Discontinued'
    clean_file.loc[clean_file['Subject'] == 104012, 'Status'] = 'Randomized'
    clean_file.loc[clean_file['Subject'] == 182008, 'Status'] = 'Randomized'
    clean_file.loc[clean_file['Subject'] == 123009, 'Status'] = 'Randomized'
    clean_file.loc[clean_file['Subject'] == 194016, 'Status'] = 'Screen Fail'
    clean_file.loc[clean_file['Subject'] == 200016, 'Status'] = 'Randomized'
    clean_file.loc[clean_file['Subject'] == 231007, 'Status'] = 'Screen Fail'
    clean_file.loc[clean_file['Subject'] == 231011, 'Status'] = 'Screen Fail'
    clean_file.loc[clean_file['Subject'] == 254002, 'Status'] = 'Discontinued'
    clean_file.loc[clean_file['Subject'] == 115012, 'Status'] = 'Screen Fail'
    clean_file.loc[clean_file['Subject'] == 112032, 'Status'] = 'Screen Fail'

    #include this visits with 'Withdarwn' Status  but payable Retention Year visits:

    clean_file = pd.concat([clean_file, include_visits], ignore_index=True)
    clean_file['Randomization'] = None

    #date format again
    clean_file['Visit Date'] = pd.to_datetime(clean_file['Visit Date'], errors='coerce')
    clean_file['Visit Date'] = clean_file['Visit Date'].dt.strftime('%m/%d/%Y')
    

    # Sort the DataFrame by 'Site', 'Subject', and 'Visit Date' columns in ascending order
    clean_file = clean_file.sort_values(by=['Site', 'Subject', 'Visit Date'], ascending=[True, True, True])
    clean_file = clean_file.iloc[:, 1:]
    clean_file = clean_file.drop(columns=['Protocol ID_y','Arm / Cohort / Type','Protocol ID'])
    clean_file['Protocol ID'] = 'NN004758'
    cols = ['Protocol ID'] + [col for col in clean_file.columns if col != 'Protocol ID']
    clean_file = clean_file[cols]

    clean_file.reset_index(drop=True, inplace=True)
    

    return(clean_file)
    