from snowflake.snowpark.context import get_active_session
import pandas as pd

session = get_active_session()

#GetProjects    
def get_projects():
    query = f"SELECT * FROM EDACONFIG.ST_EDA_VW_PROJECTS"
    return session.sql(query).to_pandas()

#GetSystems
def get_systems():
    query = f"SELECT * FROM EDACONFIG.ST_EDA_VW_SYSTEMS"
    return session.sql(query).to_pandas()

#GetDatabases()
def get_databases(system):
    if system:
        id = system["ID"]
        query = f"SELECT * FROM EDACONFIG.ST_EDA_VW_DATABASES WHERE SYSTEM_ID ='{id}'"
        return session.sql(query).to_pandas()
    return pd.DataFrame()
    
#GetSchemas()
def get_schemas(database):
    if database:
        id = database["ID"]
        # Corrected query to reference the fully qualified table name
        query =f"SELECT SCHEMA_ID,NAME FROM EDACONFIG.SCHEMAS WHERE DATABASE_ID = '{id}'"
        return session.sql(query).to_pandas()
    return pd.DataFrame()

#GetTables()
def get_tables(schema):
    if schema:
        id=schema["SCHEMA_ID"]
        query =f"SELECT * FROM EDACONFIG.ST_EDA_VW_GET_TABLES WHERE Schema_ID = '{id}'"
        return session.sql(query).to_pandas()
    return pd.DataFrame()

#GetExistingMappings()
def get_mapping(table,project):
    if table and project:
        tableID = table["ID"]
        projectID = project["ID"]
        query =f"SELECT * FROM EDACONFIG.ST_EDA_VW_MAPPING_MASTER WHERE SOURCE_TABLE_ID  = '{tableID}' and PROJECT_ID = '{projectID}'"
        return session.sql(query).to_pandas()
    return pd.DataFrame(columns=['MAPPING_ID','SOURCE','TARGET'])

def get_column_mappings(mapping,df):
    
    if mapping and len(mapping["selection"]["rows"])>0.:
        selectedRow = df.loc[mapping["selection"]["rows"]].to_dict('records')
        mappingID = selectedRow[0]["MAPPING_ID"]
        tableID = selectedRow[0]['SOURCE_TABLE_ID']
        query =f"SELECT * FROM EDACONFIG.ST_EDA_GET_COLUMN_MAPPING WHERE TABLE_ID  = '{tableID}'"
        result_df = session.sql(query).to_pandas()
        # Add the 'IsMapped' column with default value of True
        result_df['IsMapped'] = True
        return result_df
    return pd.DataFrame(columns=['MAPPING_ID','SOURCE','TARGET'])
    
def get_mapped_values(selectedCol):
    if selectedCol and selectedCol["MAPPING_COLUMN_ID"]:
        id = selectedCol["MAPPING_COLUMN_ID"]
        query = f"select * from ST_EDA_GET_TRANSLATION_VALUES WHERE MAPPING_COLUMN_ID = '{id}'"
        return session.sql(query).toPandas()
    else:
        result = pd.DataFrame(columns=['MAPPING_COLUMN_ID','FromValue','ToValue'])
        return result

def get_all_columns(table):
    if table:
        id=table["TABLE_ID"]
        query =f"SELECT Table_ID,Name FROM \"COULUMN\" WHERE Table_ID = '{id}'"
        return session.sql(query).to_pandas()
    return pd.DataFrame()

#SAVE FUNCTIONS___________________________________________
def create_mapping_master(table,project,database,schema):
    if project and table:
        projID = project["ID"]
        tableID = table["ID"]
        databaseID = database["ID"]
        schemaID = schema["SCHEMA_ID"]
        query = f"CALL EDACONFIG.Create_mapping_master('{projID}','{tableID}','{databaseID}','{schemaID}')"
        return session.sql(query).collect()
    return False
    
def save_mapping_master(mappingID,db,schema,object,objectType):
    if mappingID:
       
        query = f"CALL update_mapping_master('{mappingID}','{db}','{schema}','{object}','{objectType}')"  
        return session.sql(query).collect()
    return False
    
def save_columns(df, mappingID):
    for index, row in df.iterrows():
        source_column_name = row['SOURCECOLUMN']
        target_column_name = row['TARGETCOLUMN']
        description = row['DESCRIPTION']
        is_mapped = row['IsMapped']
        mapping_column_id = row['MAPPING_COLUMN_ID']
        
        if is_mapped and pd.isna(mapping_column_id):
            # Insert new column mapping
            query = f"CALL INSERT_MAPPING_COLUMN('{mappingID}', '{source_column_name}', '{target_column_name}', '{description}')"
            session.sql(query).collect()
        elif not pd.isna(mapping_column_id) and is_mapped == False:
            # Delete existing column mapping
            query = f"CALL DELETE_MAPPING_COLUMN('{mapping_column_id}')"
            session.sql(query).collect()
        elif not pd.isna(mapping_column_id) and is_mapped == True:
            # Update existing column mapping
            query = f"CALL UPDATE_MAPPING_COLUMN('{mapping_column_id}', '{target_column_name}', '{description}')"
            session.sql(query).collect()
    return True

def preview_sql(mapping, column_df, value_df):
    target_object_type = mapping['TARGETTYPE']
    target_db = mapping['TARGETDB']
    target_schema = mapping['TARGETSCHEMA']
    target_object_name = mapping['TARGETOBJECT']
    source_table_id = mapping['SOURCE_TABLE_ID']
    source_table_info = session.sql(f"SELECT T.NAME, S.NAME AS SCHEMA_NAME, D.NAME AS DB_NAME FROM TABLES T JOIN SCHEMAS S ON T.SCHEMA_ID = S.SCHEMA_ID JOIN DATABASES D ON S.DATABASE_ID = D.DATABASE_ID WHERE T.TABLE_ID = '{source_table_id}'").to_pandas().iloc[0]
    source_full_table_name = f"{source_table_info['DB_NAME']}.{source_table_info['SCHEMA_NAME']}.{source_table_info['NAME']}"

    select_list = []
    
    # Filter for mapped columns only
    mapped_columns = column_df[column_df['IsMapped'] == True]
    
    for index, row in mapped_columns.iterrows():
        source_col = row['SOURCECOLUMN']
        target_col = row['TARGETCOLUMN']
        
        # Check for value translations
        value_translation_exists = len(value_df[value_df['MAPPING_COLUMN_ID'] == row['MAPPING_COLUMN_ID']]) > 0
        
        if value_translation_exists:
            case_statement = f"CASE\n"
            for val_index, val_row in value_df[value_df['MAPPING_COLUMN_ID'] == row['MAPPING_COLUMN_ID']].iterrows():
                case_statement += f"    WHEN {source_col} = '{val_row['FROMVALUE']}' THEN '{val_row['TOVALUE']}'\n"
            case_statement += f"    ELSE {source_col}\n"
            case_statement += f"END AS {target_col}"
            select_list.append(case_statement)
        else:
            select_list.append(f"{source_col} AS {target_col}")

    select_clause = ",\n".join(select_list)
    
    sql_statement = f"CREATE OR REPLACE {target_object_type} {target_db}.{target_schema}.{target_object_name} AS\n"
    sql_statement += f"SELECT\n{select_clause}\nFROM {source_full_table_name};"
    
    return sql_statement

def process_cols(columns):
    i = 0
    stmt = ""
    for c in columns:
        if i == 0:
            stmt = "UPDATE " + tabname + " SET " + c + " = '" + columns[c] + "'"
            i = 5
        else:
            stmt = stmt + ", " + c + " = '" + columns[c] + "'"
    return stmt

def select_cols (df, idx):
    first = True
    stmt = ""
    cols = list(df.columns.values)
    for col in cols:
        if first:
            stmt = " WHERE " + col + " = '" + str(df.iloc[idx][col]) + "'"
            first = False
        else:
            if str(df.iloc[idx][col]) == 'None':
                stmt = stmt + " AND " + col + " IS NULL "
            else:
                stmt = stmt + " AND " + col + " = '" + str(df.iloc[idx][col]) + "'"
    return stmt

def insert_cols(cols):
    first = True
    stmt = ""
    vals = ""
    for col in cols:
        if first:
            stmt = "INSERT INTO " + tabname + " ( " + col 
            vals = " VALUES ('" + str(cols[col]) + "'"
            first = False
        else:
            stmt = stmt + ", " + col 
            vals = vals + ", '" + str(cols[col]) + "'"
    return stmt + ") " + vals + ")"

def delete_cols(idx, df):
    first = True
    stmt = ""
    cols = list(df.columns.values)
    for col in cols:
        if first:
            stmt = "DELETE FROM " + tabname + " WHERE " + col + " = '" + str(df.iloc[idx][col]) + "'"
            first = False
        else:
            if str(df.iloc[idx][col]) == 'None':
                stmt = stmt + " AND " + col + " IS NULL "
            else:
                stmt = stmt + " AND " + col + " = '" + str(df.iloc[idx][col]) + "'"
    return stmt
