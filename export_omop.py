import polars as pl
import time
from utils import get_connection_context
from tqdm import tqdm
from pathlib import Path

NB_PERSON_LIMIT = 20
FILEFORMAT = 'csv'
BATCH_SIZE = 500000
OUTPUT_DIR = Path('csv')

def save_to_file(df, table):
    
    if FILEFORMAT == 'parquet':
        df.to_parquet(OUTPUT_DIR/f'{table}.parquet', index=False, compression='zstd')
    elif FILEFORMAT == 'csv':
        df.to_csv(OUTPUT_DIR/f'{table}.csv', index=False)



def save_omop_table(df, table_name):
    print(f'Saving {table_name}, shape: {df.shape[0]}')
    time_start = time.time()
    columns_to_drop = [column for column in df.columns if 'XTN' in column or 'AIR_' in column]
    df = df.drop(columns_to_drop)
    df = df.collect()
    print(f'Fetching data: {time.time()-time_start} s')

    df.columns = df.columns.str.lower()
    if 'visit_occurrence_id' in df.columns:
        df = df.astype({'visit_occurrence_id': str})

    time_start = time.time()
    save_to_file(df, table_name)
    print(f'Saving data: {time.time()-time_start:3f} s \n')




def save_omop_table_batch(table_name, conn, num_rows):
    print(f'Saving batched {table_name}, shape: {num_rows}')
    offset = 0 
    with tqdm(total=num_rows // BATCH_SIZE + 1, desc="Processing", unit="batch") as pbar:
        # Loop through the data in chunks
        while offset < num_rows:
            # Create the query with limit and offset
            
            query = f'''
            select * from cdmphi.{table_name}
            where person_id in 
            (
            select DISTINCT person_id from kwasia01.covid_visits
            limit {NB_PERSON_LIMIT}
            )
            LIMIT {BATCH_SIZE}
            OFFSET {offset}
            '''
            # Fetch the batch of data
            df = conn.sql(query)
            
            columns_to_drop = [column for column in df.columns if 'XTN' in column or 'AIR_' in column]
            df = df.drop(columns_to_drop)
            # Convert the HANA DataFrame to a Pandas DataFrame
            df = df.collect()
            
            path = OUTPUT_DIR / f'{table_name}.csv'
            # Append to CSV file
            if offset == 0:
                # First batch, write the file and include the header
                df.to_csv(path, mode='w', index=False, header=True)
            else:
                # Subsequent batches, append and don't include the header
                df.to_csv(path, mode='a', index=False, header=False)
            
            # Increment the offset for the next batch
            offset += BATCH_SIZE
            
            # Update the progress bar
            pbar.update(1)
    print('\n')

def export_omop():
    conn = get_connection_context()
    tables = ['person', 'drug_exposure', 'visit_occurrence', 'condition_occurrence', 'death',
              'procedure_occurrence', 'device_exposure', 'measurement', 'observation', 'visit_detail']
    for table_name in tables:
        print(f'Exporting {table_name}')

        query = f'''
        select * from cdmphi.{table_name}
        where person_id in 
        (
        select DISTINCT person_id from kwasia01.covid_visits
        limit {NB_PERSON_LIMIT}
        )
        '''
        df = conn.sql(query)
        num_rows = df.shape[0]

        if  num_rows == 0:
            print(f'Empty table {table_name}\n')
            continue
        if num_rows <= BATCH_SIZE:
            save_omop_table(df, table_name)
        else:
            save_omop_table_batch(table_name, conn, num_rows)







def export_concept():
    conn = get_connection_context()
    print(f'Exporting concept.csv')
    query = f'''
    select * from cdmphi.concept
    '''
    df = conn.sql(query)
    columns_to_drop = [column for column in df.columns if 'XTN' in column or 'AIR_' in column]
    df = df.drop(columns_to_drop)
    df = df.collect()
    df.columns = df.columns.str.lower()
    df['concept_name'] = df['concept_name'].replace('','NONE') # meds_etl raise an error when there is None value

    # Sort the DataFrame: contains letters comes first, to make meds_etl identify the type as string
    df['contains_letters'] = df['concept_code'].str.contains('[a-zA-Z]', regex=True).astype(int)
    df = df.sort_values(by='contains_letters', ascending=False).drop(columns='contains_letters').reset_index(drop=True)

    save_to_file(df, 'concept')


def export_concept_relationship():
    conn = get_connection_context()
    print(f'Exporting concept_relationship.csv')
    query = f'''
        select * from cdmphi.concept_relationship
        where concept_id_1 != concept_id_2
        and relationship_id='Maps to'
    '''
    df = conn.sql(query).collect()
    df.columns = df.columns.str.lower()
    save_to_file(df, 'concept_relationship')


def export_cdm_source():
    conn = get_connection_context()
    print(f'Exporting cdm_source')
    query = f'''
        select * from cdmphi.cdm_source
    '''
    df = conn.sql(query).collect()
    df.columns = df.columns.str.lower()
    save_to_file(df, 'cdm_source')




if __name__ == "__main__":

    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True)
    export_omop()
    export_concept()
    export_concept_relationship()
    export_cdm_source()
