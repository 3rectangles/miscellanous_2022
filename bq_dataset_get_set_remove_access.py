#!/data/ext/virtualenv/venv/bin/python

import gspread
import logging
import argparse
import numpy as np
import pandas as pd
from tqdm import tqdm
from google.cloud.bigquery.table import _EmptyRowIterator
from gspread_formatting import *
from gspread_dataframe import set_with_dataframe
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from google.cloud.bigquery.enums import EntityTypes
from gspread_formatting.dataframe import format_with_dataframe, BasicFormatter


# CREDENTIALS_PATH = '/home/Jay/Desktop/PII/bq_qa.json'
CREDENTIALS_PATH = '/home/play/PII/bq_qa.json'
PROJECT_ID = 'intense-nexus-126408'

SHEET_FILE_NAME = 'BQ PII Access'
DATASET_SHEET_NAME = 'Dataset Access'
TABLE_SHEET_NAME = 'Table Access'


ROLES = {
    'METADATA_VIEWER': 'roles/bigquery.metadataViewer',     # Only view the metadata of Dataset/Table
    'READER': 'roles/bigquery.dataViewer',                  # Read the data & view metadata of Dataset/Table
    'WRITER': 'roles/bigquery.dataEditor',                  # Read & edit data + Metadata of Dataset/Table
    'JOB_USER': 'roles/bigquery.jobUser',                   # Run jobs, including queries
    'USER': 'roles/bigquery.user',                          # Read data + Metadata of datasets
    'OWNER': 'roles/bigquery.dataOwner',                    # Read, update, and delete the datasets, create, update, get, and delete the dataset's tables.
}

def get_roles(key):
    a = [x.strip() for x in key.split(',')]
    b = [ROLES[x] for x in a]
    a.extend(b)
    return a

ALL_ROLES = list(ROLES.keys())

ENTITY_TYPES = {
    'user': EntityTypes.USER_BY_EMAIL,
    'group': EntityTypes.GROUP_BY_EMAIL
}


logger = logging.getLogger(__name__)


def rprint(string, *args):
    print('\033[31m', string, '\033[0m', flush=True, *args)

def gprint(string, *args):
    print('\033[32m', string, '\033[0m', flush=True, *args)

def bprint(string, *args):
    print('\033[34m', string, '\033[0m', flush=True, *args)

def print_df(df):
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    #     print(df)
    print(df.to_markdown())




class BqConnection:
    bq_client = None

    def __init__(self, project_id: str=PROJECT_ID, credentials_fpath: str=CREDENTIALS_PATH):
        self.project_id = project_id
        self.credentials = Credentials.from_service_account_file(credentials_fpath)

    def get_connection(self):
        if BqConnection.bq_client is None:
            conn = bigquery.Client(project=self.project_id)
            BqConnection.bq_client = conn
        return BqConnection.bq_client

    def get_dataset(self, dataset_id):
        return self.get_connection().get_dataset(dataset_id)

    def get_table(self, table_id):
        return self.get_connection().get_table(table_id)

    def get_dataset_access_entries(self, dataset_id):
        return list(self.get_dataset(dataset_id).access_entries)


def get_access_entries_df(access_entries: list, dataset_id: str) -> pd.DataFrame:
    for i in range(len(access_entries)):
        access_entry = {}
        access_entry['dataset'] = dataset_id
        access_entry['email'] = access_entries[i].entity_id
        access_entry['entity_type'] = access_entries[i].entity_type
        access_entry['role'] = access_entries[i].role
        access_entries[i] = access_entry

    return pd.DataFrame(access_entries)




def get_dataset_access(dataset_id: str, email_ids: str='', roles: str='', entity_types: str='user'):
    conn = BqConnection()
    dataset_entries = conn.get_dataset_access_entries(dataset_id)
    df = get_access_entries_df(dataset_entries, dataset_id)

    email_ids = [x.strip() for x in email_ids.split(',') if x]
    if roles in ['', 'ALL']:
        roles = []
    else:
        roles = [x.strip() for x in roles.split(',') if x]
    entity_types = [ENTITY_TYPES[x] for x in entity_types.split(',') if x]

    if email_ids:
        df = df[df['email'].isin(email_ids)]

    if roles:
        df = df[df['role'].isin(roles)]

    if entity_types:
        df = df[df['entity_type'].isin(entity_types)]

    df.reset_index(drop=True, inplace=True)
    return df


def get_multiple_dataset_access_entries(dataset_ids, email_ids: str='', roles: str='', entity_types: str='user'):

    dataset_ids = [x.strip() for x in dataset_ids.split(',') if x]
    datasets_entry = pd.DataFrame()

    for dataset_id in dataset_ids:
        dataset_entry = get_dataset_access(dataset_id, email_ids, roles, entity_types)
        datasets_entry = pd.concat([datasets_entry, dataset_entry])

    datasets_entry.reset_index(drop=True, inplace=True)

    return datasets_entry


def get_all_dataset_access_entries(email_ids: str='', roles: str='', entity_types: str='user') -> pd.DataFrame:
    conn = BqConnection()
    datasets = conn.get_connection().list_datasets()
    datasets_entry = pd.DataFrame()

    for dataset in datasets:
        dataset_entry = get_dataset_access(dataset.dataset_id, email_ids, roles, entity_types)
        datasets_entry = pd.concat([datasets_entry, dataset_entry])

    datasets_entry.reset_index(drop=True, inplace=True)

    return datasets_entry


def set_dataset_access(dataset_id: str, email_id: str, role: str, entity_type: str='user',
                       update_sheet=True, file_name=None, dataset_sheet_name=None):
    print(f"Setting dataset access: {dataset_id}, user:{email_id}, entity_type:{entity_type}, role:{role}", end=' => ')

    conn = BqConnection()
    dataset = conn.get_dataset(dataset_id)
    access_entries = conn.get_dataset_access_entries(dataset_id)

    access_entries.append(
        bigquery.AccessEntry(
            role=ROLES[role],
            entity_type=ENTITY_TYPES[entity_type],
            entity_id=email_id,
        )
    )

    dataset.access_entries = access_entries
    conn.get_connection().update_dataset(dataset, ["access_entries"])  # Make an API request.

    if update_sheet:
        update_dataset_access_sheet(file_name, dataset_sheet_name, dataset_id, role, email_id, entity_type, True)

    rprint("DONE")


def set_multiple_dataset_access(dataset_ids: str='', email_ids: str='', roles: str='', entity_type: str='user',
                                update_sheet=True, file_name=None, dataset_sheet_name=None):
    dataset_ids = list(map(str.strip, dataset_ids.split(',')))
    email_ids = list(map(str.strip, email_ids.split(',')))
    if roles in ['', 'ALL']:
        roles = ALL_ROLES
    else:
        roles = [x.strip() for x in roles.split(',') if x]

    for dataset_id in dataset_ids:
        for email_id in email_ids:
            for role in roles:
                set_dataset_access(dataset_id, email_id, role, entity_type,
                                   update_sheet, file_name, dataset_sheet_name)


def set_all_dataset_access(email_ids, roles, entity_type: str='user',
                           update_sheet=True, file_name=None, dataset_sheet_name=None):
    conn = BqConnection()
    datasets = conn.get_connection().list_datasets()
    dataset_ids = ",".join([dataset.dataset_id for dataset in datasets])
    set_multiple_dataset_access(dataset_ids, email_ids, roles, entity_type,
                                update_sheet, file_name, dataset_sheet_name)


def remove_dataset_access(dataset_id, email_id, roles: str, entity_type: str='user',
                          update_sheet=True, file_name=None, dataset_sheet_name=None):
    print(f"Removing dataset access: {dataset_id}, {entity_type}:{email_id}, entity_type:{entity_type}, role:{roles}", end=' => ')

    conn = BqConnection()
    dataset = conn.get_dataset(dataset_id)
    access_entries = list(dataset.access_entries)

    if roles in ['', 'ALL']:
        roles = ALL_ROLES
    else:
        roles = [x.strip() for x in roles.split(',') if x]

    entries_to_remove = []
    for entry in access_entries:
        if entry.entity_id == email_id and entry.entity_type == ENTITY_TYPES[entity_type]:
            if not roles or entry.role in sum(list(map(get_roles, roles)), []):
                entries_to_remove.append(entry)

    if entries_to_remove:
        final_access_entries = list(set(access_entries)-set(entries_to_remove))
        dataset.access_entries = final_access_entries

        conn.get_connection().update_dataset(dataset, ["access_entries"])  # Make an API request.
        gprint("DONE")

    if update_sheet:
        for role in roles:
            update_dataset_access_sheet(file_name, dataset_sheet_name, dataset_id, role, email_id, entity_type, False)


    else:
        gprint("No Entries found to remove. ")


def remove_multiple_dataset_access(dataset_ids, email_ids, roles: str='',
                                   entity_type: str='user',
                                   update_sheet=True, file_name=None, dataset_sheet_name=None):
    dataset_ids = [x.strip() for x in dataset_ids.split(',') if x]
    email_ids = [x.strip() for x in email_ids.split(',') if x]

    for dataset_id in dataset_ids:
        for email_id in email_ids:
            remove_dataset_access(dataset_id, email_id, roles, entity_type,
                               update_sheet, file_name, dataset_sheet_name)


def remove_all_dataset_access(email_ids, roles: str='', entity_type: str='user',
                              update_sheet=True, file_name=None, dataset_sheet_name=None):
    conn = BqConnection()
    datasets = conn.get_connection().list_datasets()
    dataset_ids = [dataset.dataset_id for dataset in datasets]
    remove_multiple_dataset_access(dataset_ids, email_ids, roles, entity_type,
                                   update_sheet, file_name, dataset_sheet_name)


def execute_query(query, location):
    conn = BqConnection().get_connection()

    print(query, end=' => ')
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query_job = conn.query(query, job_config=job_config, location=location)
    result = query_job.result()
    if isinstance(result, _EmptyRowIterator):
        gprint("DONE")
    else:
        for row in result:
            print(row)


""" No use Right now. Might be used in future

def grant_dataset_access(dataset_id, email_id, role, entity_type = 'user'):
    location = BqConnection().get_dataset(dataset_id).location
    query = f"GRANT `{role}` ON SCHEMA `{dataset_id}` TO '{entity_type}:{email_id}';"
    execute_query(query, location)

def revoke_dataset_access(dataset_id, email_id, role, entity_type = 'user'):
    location = BqConnection().get_dataset(dataset_id).location
    query = f"REVOKE `{role}` ON SCHEMA `{dataset_id}` FROM '{entity_type}:{email_id}';"
    execute_query(query, location)

"""


def grant_table_access(table_id, email_id, roles, entity_type: str='user',
                       update_sheet=True, file_name=None, table_sheet_name="Table Access"):
    location = BqConnection().get_table(table_id).location
    table_sheet_name = "Table Access"
    # print("\n\n\nxxxxxxxxxxxxx\n\n", table_sheet_name)
    queries = []
    roles = [x.strip() for x in roles.split(",") if x]

    for role in roles:
        query = f"GRANT `{ROLES[role]}` ON TABLE `{table_id}` TO '{entity_type}:{email_id}';"
        queries.append(query)
    query = " ".join(queries)

    execute_query(query, location)

    if update_sheet:
        for role in roles:
            update_table_access_sheet(file_name, table_sheet_name, table_id, role, email_id, entity_type, True)

def grant_multiple_table_access(table_id, email_ids, roles, entity_type: str='user',
                                update_sheet=True, file_name=None, table_sheet_name=None):
    if roles in ['', 'ALL']:
        roles = ALL_ROLES
    else:
        roles = [x.strip() for x in roles.split(',') if x]
    email_ids = [x.strip() for x in email_ids.split(',') if x]

    for email_id in email_ids:
        for role in roles:
            grant_table_access(table_id, email_id, role, entity_type, update_sheet, file_name, table_sheet_name)


def revoke_table_access(table_id, email_id, roles: str='', entity_type='user',
                        update_sheet=True, file_name=None, table_sheet_name=None):
    location = BqConnection().get_table(table_id).location
    table_sheet_name = "Table Access"
    queries = []
    if roles in ['', 'ALL']:  # Remove all roles
        roles = ALL_ROLES
    else:
        roles = [x.strip() for x in roles.split(",") if x]

    for role in roles:
        query = f"\nREVOKE `{ROLES[role]}` ON TABLE `{table_id}` FROM '{entity_type}:{email_id}';"
        queries.append(query)
    query = " ".join(queries)

    execute_query(query, location)

    if update_sheet:
        for role in roles:
            update_table_access_sheet(file_name, table_sheet_name, table_id, role, email_id, entity_type, False)

def revoke_multiple_table_access(table_ids, email_ids, roles, entity_type: str='user',
                                update_sheet=True, file_name=None, table_sheet_name=None):
    if roles in ['','ALL']:
        roles = ALL_ROLES
    else:
        roles = [x.strip() for x in roles.split(',') if x]

    table_ids = [x.strip() for x in table_ids.split(',') if x]
    email_ids = [x.strip() for x in email_ids.split(',') if x]

    for table_id in table_ids:
        for email_id in email_ids:
            for role in roles:
                revoke_table_access(table_id, email_id, role, entity_type, update_sheet, file_name, table_sheet_name)



# Start Reading Access Sheet

class Sheet:
    client = None

    def __init__(self, file_name, credentials_fpath: str=CREDENTIALS_PATH):
        if file_name is None:
            file_name = SHEET_FILE_NAME
        self.file_name = file_name
        self.credentials_fpath = credentials_fpath

    def get_client(self):
        if Sheet.client is None:
            client = gspread.service_account(self.credentials_fpath)
            Sheet.client = client
        return Sheet.client

    def get_workbook(self):
        return self.get_client().open(self.file_name)

    def get_sheets(self):
        return self.get_workbook().worksheets()

    def get_sheet(self, sheet_name):
        return self.get_workbook().worksheet(sheet_name)

    def get_sheet_access_df(self, sheet_name):
        return self.get_all_records_df(sheet_name)

    def get_all_records_df(self, sheet_name):
        sheet_instance = self.get_sheet(sheet_name)
        df = pd.DataFrame(sheet_instance.get_all_records())
        df = df.set_index([''])
        df = df.replace(r'^\s*$', np.nan, regex=True)
        df = df.fillna(method='ffill')
        return df

    def set_dataframe_sheet(self, sheet_name, result_df: pd.DataFrame):
        sheet = self.get_sheet(sheet_name)

        result_df = result_df.mask(result_df == result_df.shift())
        result_df['Result'] = result_df['Result'].fillna(method='ffill')
        result_df['Entity Type'] = result_df['Entity Type'].fillna(method='ffill')
        result_df = result_df.fillna('')
        result_df = result_df.reset_index(drop=True)

        formatter = BasicFormatter(
            header_background_color=Color(0,0,0),
            header_text_color=Color(1,1,1)
        )
        # Unmerge all cells
        self.unmerge_cells(sheet)

        # Set values in sheet with formatting
        set_with_dataframe(sheet, result_df, resize=True, include_index=True, include_column_header=True)
        format_with_dataframe(sheet, result_df, formatter, include_index=True, include_column_header=True)

        # Merge cells (Use with caution, may cause QUOTA_LIMIT_EXCEEDED)
        # self.merge_cells(sheet, result_df, ['', 'Dataset', 'Role'])

    def merge_cells(self, sheet: gspread.worksheet.Worksheet, result_df: pd.DataFrame, cols_to_merge: list[str]):
        result_df.reset_index(drop=True)

        try:

            for col in cols_to_merge:
                index = result_df.columns.get_loc(col)
                ranges = list(result_df[result_df[col] != ''].index)
                gprint(f"Ranges: {ranges}")
                for row in range(len(ranges)-1):
                    bprint("Merge: {},{},{},{}".format(ranges[row]+2, index+1, ranges[row+1]+1, index+1))
                    sheet.merge_cells(ranges[row]+2, index+1, ranges[row+1]+1, index+1)
                gprint("Merge: {},{},{},{}".format(ranges[-1]+2, index+1, result_df.shape[0]+1, index+1))
                sheet.merge_cells(ranges[-1]+2, index+1, result_df.shape[0]+1, index+1)

        except Exception as ex:
            logger.exception(ex)
            self.unmerge_cells(sheet)

    def unmerge_cells(self, sheet: gspread.worksheet.Worksheet):
        requests = [
            {
                "unmergeCells": {
                    "range": {
                        "sheetId": sheet.id
                    }
                }
            }
        ]
        self.get_workbook().batch_update({"requests": requests})


def set_sheet_dataset_access(sheet, dataset_sheet_name):
    print(f"Setting sheet dataset access: {dataset_sheet_name}")
    df = sheet.get_sheet_access_df(dataset_sheet_name)

    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        try:
            # First remove the existing access
            remove_dataset_access(
                dataset_id=row['Dataset'],
                email_id=row['Email'],
                roles='',                          # Removes all roles
                entity_type=row['Entity Type'],
                update_sheet=False
            )

            # Then set the new access
            set_dataset_access(
                dataset_id=row['Dataset'],
                email_id=row['Email'],
                role=row['Role'],
                entity_type=row['Entity Type'],
                update_sheet=False
            )

            df.at[index, 'Result'] = 'Done'

        except Exception as e:
            df.at[index, 'Result'] = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
            print(e.__str__())

    sheet.set_dataframe_sheet(dataset_sheet_name, df)


def set_sheet_table_access(sheet, table_access_name):
    print(f"Setting sheet table access: {table_access_name}")
    df = sheet.get_sheet_access_df(table_access_name)

    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        try:
            # First remove the existing access
            revoke_table_access(
                table_id=row['Table'],
                email_id=row['Email'],
                roles='ALL',                          # Removes all roles
                entity_type=row['Entity Type'],
                update_sheet=False
            )

            # Then set the new access
            grant_table_access(
                table_id=row['Table'],
                email_id=row['Email'],
                roles=row['Role'],
                entity_type=row['Entity Type'],
                update_sheet=False
            )

            df.at[index, 'Result'] = 'Done'

        except Exception as e:
            df.at[index, 'Result'] = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
            print(e.__str__())

    sheet.set_dataframe_sheet(table_access_name, df)


def sync_sheet_access(project_id: str=PROJECT_ID,
        file_name=SHEET_FILE_NAME,
        dataset_sheet_name=DATASET_SHEET_NAME,
        table_sheet_name=TABLE_SHEET_NAME):


    print(f"Syncing access sheet :: project: {project_id},"
          f"file_name:{file_name}, dataset_sheet_name: {dataset_sheet_name},  table_sheet_name: {table_sheet_name}")

    sheet = Sheet(file_name)

    if dataset_sheet_name != '':
        # Sync dataset sheet
        set_sheet_dataset_access(sheet, dataset_sheet_name)

    if table_sheet_name != '':
        # Sync table sheet
        set_sheet_table_access(sheet, table_sheet_name)


def update_dataset_access_sheet(file_name, dataset_sheet_name,
                        dataset_id, role: str, email_id, entity_type,
                        add_or_remove):
    file_name = SHEET_FILE_NAME if file_name is None else file_name
    dataset_sheet_name = DATASET_SHEET_NAME if dataset_sheet_name is None else dataset_sheet_name

    print(f"Updating access sheet :: dataset_id: {dataset_id}, role: {role}, email_id: {email_id}, entity_type: {entity_type}")

    sheet = Sheet(file_name)
    df = sheet.get_sheet_access_df(dataset_sheet_name)

    entry_to_add = pd.DataFrame([{
        'Dataset': dataset_id,
        'Role': role,
        'Email': email_id,
        'Entity Type': entity_type
    }])

    if add_or_remove:
        dataset_df = df[df['Dataset'] == dataset_id]
        dataset_df_ = df[df['Dataset'] != dataset_id]

        if dataset_df.empty:
            dataset_df = pd.concat([dataset_df, entry_to_add])
        else:
            role_df = dataset_df[dataset_df['Role'] == role]
            role_df_ = dataset_df[dataset_df['Role'] != role]

            if role_df.empty:
                role_df = pd.concat([role_df, entry_to_add])
            else:
                entry_df = role_df[(role_df['Email'] == email_id) & (role_df['Entity Type'] == entity_type)]

                if not entry_df.empty:
                    print("Entry already exists in Table")
                else:
                    role_df = pd.concat([role_df, entry_to_add])
                    role_df = role_df.fillna(method='ffill')

            dataset_df = pd.concat([role_df, role_df_])

        df = pd.concat([dataset_df_, dataset_df])
        sheet.set_dataframe_sheet(dataset_sheet_name, df)

    else:
        existing_row_df = df[
            (df['Dataset'] == dataset_id) &
            (df['Role'] == role) &
            (df['Email'] == email_id) &
            (df['Entity Type'] == entity_type)
        ]
        if existing_row_df.empty:
            print("Entry doesn't exist in sheet")
        else:
            df = df.drop(existing_row_df.index)
        sheet.set_dataframe_sheet(dataset_sheet_name, df)

    rprint("DONE")

def update_table_access_sheet(file_name, table_sheet_name,
                                table_id, role: str, email_id, entity_type,
                                add_or_remove):
    file_name = SHEET_FILE_NAME if file_name is None else file_name
    table_sheet_name = DATASET_SHEET_NAME if table_sheet_name is None else table_sheet_name

    print(f"Updating access sheet :: dataset_id: {table_id}, role: {role}, email_id: {email_id}, entity_type: {entity_type}")


    sheet = Sheet(file_name)

    df = sheet.get_sheet_access_df(table_sheet_name)
    # print(df, end="\n")



    entry_to_add = pd.DataFrame([{
        'Table': table_id,
        'Role': role,
        'Email': email_id,
        'Entity Type': entity_type
    }])
    print(entry_to_add)

    if add_or_remove:
        dataset_df = df[df['Table'] == table_id]
        dataset_df_ = df[df['Table'] != table_id]

        if dataset_df.empty:
            dataset_df = pd.concat([dataset_df, entry_to_add])
        else:
            role_df = dataset_df[dataset_df['Role'] == role]
            role_df_ = dataset_df[dataset_df['Role'] != role]

            if role_df.empty:
                role_df = pd.concat([role_df, entry_to_add])
            else:
                entry_df = role_df[(role_df['Email'] == email_id) & (role_df['Entity Type'] == entity_type)]

                if not entry_df.empty:
                    print("Entry already exists in Table")
                else:
                    role_df = pd.concat([role_df, entry_to_add])
                    role_df = role_df.fillna(method='ffill')

            dataset_df = pd.concat([role_df, role_df_])

        df = pd.concat([dataset_df_, dataset_df])
        sheet.set_dataframe_sheet(table_sheet_name, df)

    else:
        existing_row_df = df[
            (df['Table'] == table_id) &
            (df['Role'] == role) &
            (df['Email'] == email_id) &
            (df['Entity Type'] == entity_type)
            ]
        if existing_row_df.empty:
            print("Entry doesn't exist in sheet")
        else:
            df = df.drop(existing_row_df.index)
        sheet.set_dataframe_sheet(table_sheet_name, df)

    rprint("DONE")



def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    subparsers = parser.add_subparsers(dest='command', help='Commands to run', required=True)

    # Get
    get_parser = subparsers.add_parser(
        'get', help=get_dataset_access.__doc__)
    get_parser.add_argument('-p', '--project_id', default=PROJECT_ID, nargs='?', const=PROJECT_ID, help="Project ID")
    get_parser.add_argument('-d', '--dataset_id', default='', const='', nargs='?', help="Dataset ID")
    get_parser.add_argument('-i', '--email_ids', default='', nargs='?', const='', help="Comma seperated Email IDs")
    get_parser.add_argument('-e', '--entity_types', default='user', const='user', nargs='?', help="Comma seperated Entity types")
    get_parser.add_argument('-r', '--roles', default='', nargs='?', const='', help="Comma seperated Roles")

    # Set
    set_parser = subparsers.add_parser(
        'set', help=set_dataset_access.__doc__)
    set_parser.add_argument('-p', '--project_id', default=PROJECT_ID, const=PROJECT_ID, nargs='?', help="Project ID")
    set_parser.add_argument('-d', '--dataset_ids', required=True, help="Comma-seperated Dataset IDs")
    set_parser.add_argument('-t', '--table_id', default='', const='', nargs='?', help="Table ID")
    set_parser.add_argument('-i', '--email_ids', required=True, help="Comma-seperated Email IDs")
    set_parser.add_argument('-e', '--entity_type', required=True, help="Entity type")
    set_parser.add_argument('-r', '--roles', required=True, help="Comma-seperated Roles")

    # Remove
    remove_parser = subparsers.add_parser(
        'remove', help=remove_dataset_access.__doc__)
    remove_parser.add_argument('-p', '--project_id', default=PROJECT_ID, const=PROJECT_ID, nargs='?', help="Project ID")
    remove_parser.add_argument('-d', '--dataset_ids', required=True, help="Comma-seperated Dataset IDs")
    remove_parser.add_argument('-t', '--table_id', default='', const='', nargs='?', help="Table ID")
    remove_parser.add_argument('-i', '--email_ids', required=True, help="Comma-seperated Email ID")
    remove_parser.add_argument('-e', '--entity_type', default='user', help="Entity type")
    remove_parser.add_argument('-r', '--roles', default='', const='', nargs='?', help="Comma-seperated Roles")

    # Sync
    sync_parser = subparsers.add_parser(
        'sync', help=sync_sheet_access.__doc__)
    sync_parser.add_argument('-p', '--project_id', default=PROJECT_ID, const=PROJECT_ID, nargs='?', help="Project ID")
    sync_parser.add_argument('-f', '--file_name', required=True, help="Google Sheet File Name")
    sync_parser.add_argument('-d', '--dataset_sheet_name', default='', const='', nargs='?', help="Dataset Sheet Name")
    sync_parser.add_argument('-t', '--table_sheet_name', default='', const='', nargs='?', help="Table Sheet Name")

    args = parser.parse_args()

    bprint(args)


    if args.command == 'get':
        if args.dataset_id == '':
            df = get_all_dataset_access_entries(
                email_ids=args.email_ids,
                roles=args.roles,
                entity_types=args.entity_types
            )
        else:
            df = get_dataset_access(
                dataset_id = args.dataset_id,
                email_ids = args.email_ids,
                roles = args.roles,
                entity_types = args.entity_types
            )

        print_df(df)


    elif args.command == 'set':
        if args.table_id == '':
            set_multiple_dataset_access(args.dataset_ids, args.email_ids, args.roles, args.entity_type)
        else:
            grant_multiple_table_access(f"{args.dataset_ids}.{args.table_id}", args.email_ids, args.roles, args.entity_type)


    elif args.command == 'remove':
        if args.table_id == '':
            remove_multiple_dataset_access(args.dataset_ids, args.email_ids, args.roles, args.entity_type)
        else:
            revoke_multiple_table_access(f"{args.dataset_ids}.{args.table_id}", args.email_ids, args.roles, args.entity_type)

    elif args.command == 'sync':
        sync_sheet_access(args.project_id, args.file_name, args.dataset_sheet_name, args.table_sheet_name)


if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        # logger.exception(ex)
        raise

