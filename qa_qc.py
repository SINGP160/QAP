# This sample code helps you get started with the custom scenario API.
#For more details and samples, please see our Documentation
#importing the required modules
from dataiku.scenario import Scenario
import dataiku
from dataiku.core.sql import SQLExecutor2
from dataiku import pandasutils as pdu
import pandas as pd
import numpy as np
import sys
import great_expectations as ge
import collections
from datetime import datetime
import multiprocessing
from multiprocessing.pool import ThreadPool
pool = ThreadPool(multiprocessing.cpu_count())

def qa_qc_checks(control_dataset,project_key,dataset_type,output_dataset,scenario_id,step,step_type):
    '''Function to perform qa_qc checks'''
    client = dataiku.api_client()
    #checks_table  = dataiku.Dataset('checks_table')
    #checks_table = checks_table.get_dataframe()
    executor = SQLExecutor2(connection='SF_VAW_REG_COMM_ANA_MED')
    clear_log_table = executor.query_to_df("select * from ANALYTICS_PROD_GLBXBUIMSETL_DEV.LOG_TABLE where to_date(end_time, 'DD/MM/YYYY HH24:MI:SS') <= dateadd('month',0, date_trunc('month',CURRENT_DATE()));")
    if not clear_log_table.empty:
        executor.query_to_df("insert into ANALYTICS_PROD_GLBXBUIMSETL_DEV.LOG_TABLE_ARCHIVE select * from ANALYTICS_PROD_GLBXBUIMSETL_DEV.LOG_TABLE where to_date(end_time, 'DD/MM/YYYY HH24:MI:SS') <= dateadd('month',0, date_trunc('month',CURRENT_DATE()));")
        executor.query_to_df("delete from ANALYTICS_PROD_GLBXBUIMSETL_DEV.LOG_TABLE where to_date(end_time, 'DD/MM/YYYY HH24:MI:SS') <= dateadd('month',0, date_trunc('month',CURRENT_DATE()));")
    checks_table = executor.query_to_df("select * from ANALYTICS_PROD_GLBXBUIMSETL_DEV.CHECKS_TABLE_CENTRAL")
    checks_table = checks_table.dropna(how='all',axis=0)
    checks_table_cols = ['Check', 'parameter', 'Function', 'Is_complete_dataset_req', 'filter_columns']
    checks_table.columns = checks_table_cols
    control_table_df = dataiku.Dataset(control_dataset)
    control_table_df = control_table_df.get_dataframe()
    control_table_df = control_table_df.dropna(how='all',axis=0)
    project = client.get_project(project_key)
    scenario = project.get_scenario(scenario_id)
    settings = scenario.get_settings()
    control_table_dataset_filters = []
    
    control_table_df = control_table_df[control_table_df.dataset_type == dataset_type].reset_index(drop = True)
    freq_file_exist_df = control_table_df[(control_table_df.checks_name == 'frequency_check') | (control_table_df.checks_name == 'file_exist')].reset_index(drop = True)
    if step_type == 'Scenario':
        #to get the datasets that were being build in the previous step i,e., a scenario
        scenario_name = settings.get_raw()['params']['steps'][step-1]['params']['scenarioId']
        scenario_in = project.get_scenario(scenario_name)
        settings_in = scenario_in.get_settings()
        for step in settings_in.get_raw()['params']['steps']:
            for i in range(len(step['params']['builds'])):
                control_table_dataset_filters.append(step['params']['builds'][i]['itemId'])
    elif step_type == 'Build':
        #to get the datasets that were being build in the previous step
        for i in range(len(settings.get_raw()['params']['steps'][step-1]['params']['builds'])):
            control_table_dataset_filters.append((settings.get_raw()['params']['steps'][step-1]['params']['builds'][i]['itemId']))
    boolean_series = control_table_df.dataset_name.isin(control_table_dataset_filters)
    control_table_df = control_table_df[boolean_series]
    control_table_df = control_table_df[boolean_series].reset_index(drop =True)
    if dataset_type == 'input':
        control_table_df = control_table_df.append(freq_file_exist_df).reset_index(drop =True)
   
    cols = control_table_df.columns.tolist()
    resultset = pd.DataFrame(columns = cols +  ['run_flag','Result','Output_Reference','start_time','end_time'])

    def run_checks_in_parallel(row_val,cols,control_table_df,checks_table):
        '''Running the all the checks specified in control table in parallel'''
        output_df = pd.DataFrame(columns = cols +  ['run_flag','Result','Output_Reference','start_time','end_time'])
        index = row_val
        output_df.at[index,'run_flag'] = 'N'
        for col in cols:
            output_df.at[index,col] = control_table_df.at[index,col]
        if control_table_df.at[index,'active'] == 'Y':
            output_df.at[index,'run_flag'] = 'Y'
            output_df.at[index,'start_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            checks_nm = control_table_df.at[index,'checks_name']
            if pd.isnull(control_table_df.at[index,'input_parameters']):
                inputs_required = ['']
            else:
                inputs_required = control_table_df.at[index,'input_parameters'].split('|')
            complete_dataset_req = (checks_table.loc[(checks_table['Check'] == checks_nm)]['Is_complete_dataset_req']).item()
            filter_req_columns_flag = (checks_table.loc[(checks_table['Check'] == checks_nm)]['filter_columns']).item()
            if checks_nm not in ['frequency_check','distinct_column_comparsion_between_datasets','file_exist']:
                dataset_name = control_table_df.at[index,'dataset_name']
                source_dataset = dataiku.Dataset(dataset_name)
                if complete_dataset_req == 'Y':
                    if filter_req_columns_flag == 'Y' and checks_nm not in ['columnA_greater_than_columnB','column_comparison']:
                        columns_req = []
                        try:
                            columns_req.extend(eval(inputs_required[0]))
                        except:
                            columns_req.append(inputs_required[0])
                        try:    
                            source_df = source_dataset.get_dataframe(columns = columns_req)
                        except:
                            source_df = source_dataset.get_dataframe()
                    elif checks_nm in ['columnA_greater_than_columnB','column_comparison']:
                        try:
                            source_df = source_dataset.get_dataframe(columns = inputs_required)
                        except:
                            source_df = source_dataset.get_dataframe()
                    else:
                        source_df = source_dataset.get_dataframe()
                else:
                    source_df = source_dataset.get_dataframe(sampling='head', limit=1)
                ge_df = ge.from_pandas(source_df)
            
            if checks_nm == "frequency_check":
                #frequency check - checking whether the dataset has been refreshed as per the frequency.
                refresh_days = int(inputs_required[0])
                dataset_names = eval(inputs_required[1])
                datetimes = []
                for dataset in dataset_names:
                    freq_df = dataiku.Dataset(dataset)
                    #getting a dataset configuration details and getting the last modified on date from that
                    info = freq_df.get_config()
                    #converting a unix timestamp into date time in python format
                    datetimes.append(datetime.utcfromtimestamp(info['versionTag']['lastModifiedOn']/1000).strftime('%Y-%m-%d'))
                dates = pd.date_range(end = datetime.today().strftime("%Y-%m-%d"), periods = refresh_days).strftime('%Y-%m-%d').tolist()
                check =  all(item in dates for item in datetimes)
                if check:
                    output_df.at[index,'Result'] = 'PASS'
                else:
                    datetimes_series = pd.Series(datetimes).isin(dates)
                    dataset_names_series = pd.Series(dataset_names)
                    unrefreshed_datasets = ','.join(dataset_names_series[~datetimes_series].tolist())
                    output_df.at[index,'Result'] = 'FAIL'
                    output_df.at[index,'Output_Reference'] = f'These datasets - {unrefreshed_datasets} are not refreshed as per the frequency'
                output_df.at[index,'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            elif checks_nm == 'file_exist':
                dataset_name = control_table_df.at[index,'dataset_name']
                handle = dataiku.Folder(dataset_name)
                paths = handle.list_paths_in_partition()
                file_name = inputs_required[0]
                res = [ele for ele in paths if(file_name in ele)]
                if res:
                    output_df.at[index,'Result'] = 'PASS'
                else:
                    output_df.at[index,'Result'] = 'FAIL'
                    output_df.at[index,'Output_Reference'] = f'There are no matched files in this {dataset_name} folder for the provided file name pattern - {inputs_required[0]}'
            elif checks_nm == 'distinct_column_comparsion_between_datasets':
                inputs_required = [eval(i) for i in inputs_required]
                column_1 = inputs_required[0][1]
                column_2 = inputs_required[1][1]
                dataset_1 = dataiku.Dataset(inputs_required[0][0])
                dataframe_1 = dataset_1.get_dataframe(columns = [column_1])
                dataset_2 = dataiku.Dataset(inputs_required[1][0])
                dataframe_2 = dataset_2.get_dataframe(columns = [column_2])
                l1 = dataframe_1[column_1].unique().tolist()
                l2 = dataframe_2[column_2].unique().tolist()
                result = all(elem in l1 for elem in l2)
                if result:
                    output_df.at[index,'Result'] = 'PASS'
                else:
                    diff_elem = set(l2).difference(set(l1))
                    output_df.at[index,'Result'] = 'FAIL'
                    output_df.at[index,'Output_Reference'] = f'{diff_elem} - these values from comparison dataset {inputs_required[0][0]} are not matching with the main dataset {inputs_required[1][0]} column values'
            elif checks_nm == 'schema_comparison':
                #schema comparison check - comparing the schema of 2 datasets
                source_info = source_dataset.get_config()
                comp_df = dataiku.Dataset(inputs_required[0])
                comp_info = comp_df.get_config()
                source_cols = []
                comp_cols = []
                #getting the schema of a dataset from its configuration details using get_config() method
                for i in range(len(source_info['schema']['columns'])):
                    source_cols.append(source_info['schema']['columns'][i]['name'])
                for i in range(len(comp_info['schema']['columns'])):
                    comp_cols.append(comp_info['schema']['columns'][i]['name'])
                if collections.Counter(source_cols) == collections.Counter(comp_cols):
                    output_df.at[index,'Result'] = 'PASS'
                else:
                    comp_set = set(comp_cols)
                    source_set = set(source_cols)
                    diff_cols = [x for x in source_set if x not in comp_set]
                    if not diff_cols:
                        diff_cols = [x for x in comp_set if x not in source_set]
                    diff_cols = ','.join(diff_cols)
                    output_df.at[index,'Result'] = 'FAIL'
                    output_df.at[index,'Output_Reference'] = f'These columns {diff_cols} are creating a schema difference between {dataset_name} and {inputs_required[0]}'
                output_df.at[index,'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            elif checks_nm == 'non_empty_table_check':
                #non empty table check - to check if the dataset is empty
                src_ds = project.get_dataset(dataset_name)
                #computing the number of records using compute metrics function
                src_metrics = src_ds.compute_metrics(metric_ids=['records:COUNT_RECORDS'])
                # if any error in computing the metrics, we are reading the whole dataset and taking count from it.
                if 'error' in src_metrics['result']['runs'][0]:
                    source_df = dataiku.Dataset(dataset_name)
                    source_df = source_df.get_dataframe()
                    row_count = source_df.shape[0]
                else:
                    for i in range(len(src_metrics['result']['computed'])):
                        if src_metrics['result']['computed'][i]['metricId'] == 'records:COUNT_RECORDS':
                            row_count = int(src_metrics['result']['computed'][i]['value'])
                if row_count > 0:
                    output_df.at[index,'Result'] = 'PASS'
                else:
                    output_df.at[index,'Result'] = 'FAIL'
                    output_df.at[index,'Output_Reference'] = f'The dataset count is 0, there might be an update in the schema of the source dataset or the source table doesnt have any data.'
                output_df.at[index,'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            elif checks_nm == 'parent_table_count_greater_than_child_table_count':
                #join check - making sure the output of join dataset is not greater than its parent dataset
                src_ds = project.get_dataset(dataset_name)
                #computing the number of records using compute metrics function
                src_metrics = src_ds.compute_metrics(metric_ids=['records:COUNT_RECORDS'])
                # if any error in computing the metrics, we are reading the whole dataset and taking count from it.
                if 'error' in src_metrics['result']['runs'][0]:
                    source_df = dataiku.Dataset(dataset_name)
                    source_df = source_df.get_dataframe()
                    source_count = source_df.shape[0]
                else:
                    for i in range(len(src_metrics['result']['computed'])):
                        if src_metrics['result']['computed'][i]['metricId'] == 'records:COUNT_RECORDS':
                            source_count = int(src_metrics['result']['computed'][i]['value'])
                comp_ds = project.get_dataset(inputs_required[0])
                tgt_metrics = comp_ds.compute_metrics(metric_ids=['records:COUNT_RECORDS'])
                if 'error' in tgt_metrics['result']['runs'][0]:
                    comp_df = dataiku.Dataset(inputs_required[0])
                    comp_df = comp_df.get_dataframe()
                    comp_count = comp_df.shape[0]
                else:
                    for i in range(len(tgt_metrics['result']['computed'])):
                        if tgt_metrics['result']['computed'][i]['metricId'] == 'records:COUNT_RECORDS':
                            comp_count = int(tgt_metrics['result']['computed'][i]['value'])
                if comp_count >= source_count:
                    output_df.at[index,'Result'] = 'PASS'
                else:
                    output_df.at[index,'Result'] = 'FAIL'
                    output_df.at[index,'Output_Reference'] = f'{dataset_name} count is {source_count} and the count of {inputs_required[0]} is {comp_count}'                    
                output_df.at[index,'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            elif checks_nm == 'row_count_comparison':
                #row count comparison - check if the row count of 2 datasets is equal and also checks the row count of stacked output is equal to sum of row counts of the input datasets
                src_ds = project.get_dataset(dataset_name)
                #computing the number of records using compute metrics function
                src_metrics = src_ds.compute_metrics(metric_ids=['records:COUNT_RECORDS'])
                if 'error' in src_metrics['result']['runs'][0]:
                    source_df = dataiku.Dataset(dataset_name)
                    source_df = source_df.get_dataframe()
                    source_count = source_df.shape[0]
                else:
                    for i in range(len(src_metrics['result']['computed'])):
                        if src_metrics['result']['computed'][i]['metricId'] == 'records:COUNT_RECORDS':
                            source_count = int(src_metrics['result']['computed'][i]['value'])
                row_counts = []
                for data in range(len(inputs_required)):
                    comp_ds = project.get_dataset(inputs_required[data])
                    tgt_metrics = comp_ds.compute_metrics(metric_ids=['records:COUNT_RECORDS'])
                    if 'error' in tgt_metrics['result']['runs'][0]:
                        comp_df = dataiku.Dataset(inputs_required[data])
                        comp_df = comp_df.get_dataframe()
                        row_counts.append(comp_df.shape[0])
                    else:
                        for i in range(len(tgt_metrics['result']['computed'])):
                            if tgt_metrics['result']['computed'][i]['metricId'] == 'records:COUNT_RECORDS':
                                row_counts.append(int(tgt_metrics['result']['computed'][i]['value']))
                sum_row_counts = sum(row_counts)
                comp_datasets = ','.join(output_df.at[index,'input_parameters'].split('|'))
                if source_count == sum(row_counts):
                    output_df.at[index,'Result'] = 'PASS'
                else:
                    output_df.at[index,'Result'] = 'FAIL'
                    if len(row_counts) > 1:
                        output_df.at[index,'Output_Reference'] = f'{dataset_name} count is {source_count} and the combined count of these datasets - {comp_datasets} is {sum_row_counts}'
                    else:
                        output_df.at[index,'Output_Reference'] = f'{dataset_name} count is {source_count} and the count of {comp_datasets} is {sum_row_counts}'                    
                output_df.at[index,'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            elif checks_nm == 'column_count_comparison':
                #column count comparison - checking the column count of 2 datasets is equal.
                column_counts = []
                for data in range(len(inputs_required)):
                    comp_df = dataiku.Dataset(inputs_required[data])
                    comp_df = comp_df.get_dataframe()
                    column_counts.append(comp_df.shape[1])
                source_count = source_df.shape[1]
                comp_datasets = ','.join(output_df.at[index,'input_parameters'].split('|'))
                str_column_counts = ','.join(list(map(str,column_counts)))
                if len(set(column_counts))==1 and source_count == column_counts[0]:
                    output_df.at[index,'Result'] = 'PASS'
                else:
                    output_df.at[index,'Result'] = 'FAIL'
                    if len(column_counts) > 1:
                        output_df.at[index,'Output_Reference'] = f'The column count of {dataset_name} is {source_count} and the column counts of these datasets - {comp_datasets} are {str_column_counts} respectively'
                    else:
                        output_df.at[index,'Output_Reference'] = f'The column count of {dataset_name} is {source_count} and the column count of {comp_datasets} is {column_counts[0]}'                    
                output_df.at[index,'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            elif checks_nm == 'multi_data_type_check':
                #multi_data_type_check- checking datatypes of multiple columns using great expectations module
                result_type = []
                result_reference = []
                columns = eval(inputs_required[0])
                datatypes = eval(inputs_required[1])
                for column,datatype in zip(columns,datatypes):
                    result = ge_df.expect_column_values_to_be_of_type(column,datatype)
                    result_type.append(result['success'])
                    if 'partial_unexpected_list' in result['result']:
                        result_reference.append(list(np.unique(np.array(result['result']['partial_unexpected_list']).astype(str))))
                    elif 'observed_value' in result['result']:
                        result_reference.append(list(np.unique(np.array(result['result']['observed_value']).astype(str))))
                if False in result_type:
                    output_df.at[index,'Result'] = 'FAIL'
                    indexes = np.array([i for i, val in enumerate(result_type) if not val])
                    result_reference = np.array(result_reference)
                    columns = np.array(columns)
                    result_reference = result_reference[indexes].tolist()
                    columns = ','.join(columns[indexes].tolist())
                    output_df.at[index,'Output_Reference'] = f'Columns - {columns} and their unexpected result of datatypes - {result_reference} respectively'
                else:
                    output_df.at[index,'Result'] = 'PASS'
                output_df.at[index,'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            else:
                #This section covers all the great expectations QA checks.
                inputs_required.extend(['','','','','','','','','','',''])
                func_name = (checks_table.loc[(checks_table['Check'] == checks_nm)]['Function']).item()
                functions_dict = {'column_count' : 'ge_df.{0}()'.format(func_name),
                         'column_comparison' : "ge_df.{0}('{1}','{2}')".format(func_name,*inputs_required),
                             'null_check' : "ge_df.{0}('{1}')".format(func_name,*inputs_required),
                             'data_type_check' : "ge_df.{0}('{1}','{2}')".format(func_name,*inputs_required),
                             'distinct_values' : "ge_df.{0}('{1}',{2})".format(func_name,*inputs_required),
                             'data_length' : "ge_df.{0}('{1}',{2})".format(func_name,*inputs_required),
                             'column_range_value' : "ge_df.{0}('{1}',{2},{3})".format(func_name,*inputs_required),
                             'column_present' : "ge_df.{0}('{1}')".format(func_name,*inputs_required),
                             'regex_match' : "ge_df.{0}('{1}',{2})".format(func_name,*inputs_required),
                             'aggregate_check_max' : "ge_df.{0}('{1}',{2},{3})".format(func_name,*inputs_required),
                             'aggregate_check_min' : "ge_df.{0}('{1}',{2},{3})".format(func_name,*inputs_required),
                             'data_type_check_gen' : "ge_df.{0}('{1}',{2})".format(func_name,*inputs_required),
                             'column_order' : "ge_df.{0}({1})".format(func_name,*inputs_required),
                             'data_length_range' : "ge_df.{0}('{1}',{2},{3})".format(func_name,*inputs_required),
                             'table_count_range' : "ge_df.{0}({1},{2})".format(func_name,*inputs_required),
                             'columns_to_match_set' : "ge_df.{0}({1})".format(func_name,*inputs_required),
                             'unique_check' : "ge_df.{0}('{1}')".format(func_name,*inputs_required),
                             'column_values_to_be_in_set' : "ge_df.{0}('{1}',{2})".format(func_name,*inputs_required),
                             'compound_columns_to_be_unique' : "ge_df.{0}({1})".format(func_name,*inputs_required),
                             'multicolumn_sum_to_equal' : "ge_df.{0}({1},{2})".format(func_name,*inputs_required),
                             'columnA_greater_than_columnB' : "ge_df.{0}('{1}','{2}')".format(func_name,*inputs_required),
                             'date_format_check' : "ge_df.{0}('{1}','{2}')".format(func_name,*inputs_required)}
                eval_f = functions_dict[checks_nm]
                res = eval(eval_f)

                if checks_nm == 'column_count':
                    output_df.at[index,'Result'] = 'PASS' if int(res) == int(control_table_df.at[index,'expected_result']) else 'FAIL'
                else:
                    output_df.at[index,'Result'] = 'PASS' if res['success'] == True else 'FAIL'
                
                #creating the output reference in below steps in case of any failure in  the GE quality check failure.
                if output_df.at[index,'Result'] == 'FAIL':
                    if type(res) == int:
                        output_df.at[index,'Output_Reference'] = str(res)
                    elif ('result' in res) and ('partial_unexpected_list' in res['result']):
                        output_df.at[index,'Output_Reference'] = str(list(np.unique(np.array(res['result']['partial_unexpected_list']).astype(str))))
                    elif ('result' in res) and ('observed_value' in res['result']):
                        output_df.at[index,'Output_Reference'] = str(list(np.unique(np.array(res['result']['observed_value']).astype(str))))
                    elif 'result' in res:
                        output_df.at[index,'Output_Reference'] = str(res['result'])
                    else:
                        output_df.at[index,'Output_Reference'] = str(res)

                output_df.at[index,'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

            if output_df.at[index,'Result'] == 'PASS':
                print(f"MESSAGE: {checks_nm} check passed for dataset - {control_table_df.at[index,'dataset_name']} and given input parameters - {control_table_df.at[index,'input_parameters']}")

            if output_df.at[index,'Result'] == 'FAIL':
                print(f"WARNING: {checks_nm} check failed for dataset - {control_table_df.at[index,'dataset_name']} and given input parameters - {control_table_df.at[index,'input_parameters']}")
        return output_df

    if any(control_table_df['priority']==1):
        # checking all the priority 1 checks first and failing the code if any of the check fails
        control_table_df_1 = control_table_df[control_table_df['priority']==1]
        row_val = control_table_df_1.index.values.tolist()
        datasets = pool.map(lambda x:run_checks_in_parallel(x,cols,control_table_df_1,checks_table),row_val)
        for x in datasets:
            resultset = resultset.append(x)
        if resultset['Result'].isin(['FAIL']).any():
            output_result = dataiku.Dataset(output_dataset)
            resultset_final = output_result.get_dataframe()
            resultset_final = resultset_final.append(resultset)
            output_result.write_with_schema(resultset_final)
            sys.exit("ERROR: Priority checks failed, Please check the log table for more details.")
    if any(control_table_df['priority']==0):
        # checking all the priority 0 checks once priority 1 checks are all passed and fails if any of the priority 0 check fails.
        control_table_df_0 = control_table_df[control_table_df['priority']==0]
        row_val = control_table_df_0.index.values.tolist()
        datasets = pool.map(lambda x:run_checks_in_parallel(x,cols,control_table_df_0,checks_table),row_val)
        for x in datasets:
            resultset = resultset.append(x)
    output_result = dataiku.Dataset(output_dataset)
    resultset_final = output_result.get_dataframe()
    resultset_final = resultset_final.append(resultset)
    output_result.write_with_schema(resultset_final)
    if resultset['Result'].isin(['FAIL']).any():
        sys.exit("ERROR: Few checks failed, Please check the log table for more details.")
