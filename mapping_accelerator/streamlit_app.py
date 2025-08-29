import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import data_access as da

# Get the current credentials from the Streamlit app's environment
session = get_active_session()

# Add the data_access.py file as a session import so it can be used
# by the app's functions. This is required for Snowflake Streamlit apps.
session.add_import("data_access.py","data_access")

# Configure the Streamlit page, setting the layout to wide and the page title.
st.set_page_config(layout="wide", page_title="GT Mapping Accelerator Mockup")

# Use a markdown block for custom CSS to style the entire application.
# This makes the app look more professional and consistent.
st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;700&display=swap');
        
        html, body, [data-testid="stAppViewContainer"] {
            font-family: 'Roboto', sans-serif;
        }

        /* Sidebar Styling */
        [data-testid=stSidebar] {
            background-color: #1a1a2e;
            color: #ffffff;
            padding-top: 3.5rem;
        }
        [data-testid=stSidebar] .stSelectbox label {
            color: #ffffff;
        }
        .stSelectbox div[role="button"] {
            background-color: #3e4c6c;
            color: #ffffff;
            border: 1px solid #5a6b8f;
        }

        /* Main Container Styling */
        [data-testid='stMainContainer'] > .st-emotion-cache-1g831rt.e1f1d6z75 {
            background-color: #f0f2f6;
            padding: 2rem;
        }
        h1, h2, h3, h4, h5, h6 {
            color: #1a1a2e;
            font-weight: 700;
        }
        
        /* Expander and Container Styling */
        .st-emotion-cache-1h6d2u6 { /* Expander header */
            background-color: #d1d8e0;
            border-radius: 8px;
            border-bottom: 2px solid #5a6b8f;
        }
        [data-testid='stExpander'] {
            background-color: #d1d8e0;
            border-radius: 8px;
            border: none;
        }
        .st-emotion-cache-19a50ma { /* Container border */
            border-radius: 8px;
            border: 1px solid #c0c0c0;
            padding: 1.5rem;
        }

        /* Button Styling */
        .stButton button {
            background-color: #007acc;
            color: white;
            border-radius: 5px;
            border: none;
            padding: 10px 20px;
            font-weight: bold;
            transition: background-color 0.2s;
        }
        .stButton button:hover {
            background-color: #005f99;
        }
        .stButton button:active {
            transform: translateY(1px);
        }

        /* Data Editor Styling */
        .st-emotion-cache-79elbk { /* Editor container */
            border-radius: 8px;
            border: 1px solid #c0c0c0;
        }

        /* Table Styling */
        .st-emotion-cache-19a50ma .stDataFrame table, .st-emotion-cache-19a50ma .stDataFrame tbody, .st-emotion-cache-19a50ma .stDataFrame thead {
            background-color: #fff;
            border-radius: 8px;
            overflow: hidden;
        }
        .st-emotion-cache-19a50ma .stDataFrame {
            border: 1px solid #e0e0e0;
            border-radius: 8px;
        }

        .stDataFrame.st-emotion-cache-2l2p9s {
            border: 1px solid #c0c0c0 !important;
            border-radius: 8px;
        }

    </style>
    """,
    unsafe_allow_html=True,
)

# Set the main title of the application.
st.title("Mapping Accelerator Mockup")

# Function to set a session state variable for new mapping creation.
def new_mapping():
    st.session_state["newMapping"] = True

# Function to check if the data editor has any changes (added, edited, or deleted rows).
def isDirty(editorKey):
    changes = st.session_state[editorKey]
    return changes["edited_rows"] or changes["added_rows"] or changes["deleted_rows"]
    
# Initialize all necessary session state variables. This ensures the app's state
# is preserved across user interactions and reruns.
if 'projectSelect' not in st.session_state:
    st.session_state['projectSelect']=None
if 'systemSelect' not in st.session_state:
    st.session_state['systemSelect']=None
if 'databaseSelect' not in st.session_state:
    st.session_state['databaseSelect']=None
if 'schemaSelect' not in st.session_state:
    st.session_state['schemaSelect'] = None
if 'tableSelect' not in st.session_state:
    st.session_state['tableSelect'] = None
if 'masterList' not in st.session_state:
    st.session_state['masterList'] = False
if 'newMapping' not in st.session_state:
    st.session_state['newMapping'] = False
if 'isDirty' not in st.session_state:
    st.session_state['isDirty'] = False
if 'colSelect' not in st.session_state:
    st.session_state['colSelect'] =None
if 'mappingSelect' not in st.session_state:
    st.session_state['mappingSelect'] =None

# Function to reset all filter-related session state variables to their default values.
def resetFilters():
    st.session_state['systemSelect']=None
    st.session_state['databaseSelect']=None
    st.session_state['schemaSelect'] = None
    st.session_state['tableSelect'] = None
    st.session_state['newMapping'] = False
    st.session_state['isDirty'] = False
    st.session_state['masterList'] = None
   

# Load data for the selectboxes from the Snowflake backend using the data_access module.
projectList = da.get_projects().to_dict('records')
systemList = da.get_systems().to_dict('records')
databaseList  = da.get_databases(st.session_state["systemSelect"]).to_dict('records')
schemaList= da.get_schemas(st.session_state["databaseSelect"]).to_dict('records')
tableList = da.get_tables(st.session_state["schemaSelect"]).to_dict('records')
mappings = da.get_mapping(st.session_state["tableSelect"],st.session_state["projectSelect"])
columnMappings = da.get_column_mappings(st.session_state["masterList"],mappings)
valueMappings = da.get_mapped_values(st.session_state["colSelect"])
mappingSelected = False

# Check if a mapping has been selected from the dataframe.
if st.session_state["masterList"]:
    mappingSelected = len(st.session_state["masterList"]["selection"]["rows"])>0
    if mappingSelected:
        selectedMapping  = mappings.loc[st.session_state["masterList"]["selection"]["rows"]].to_dict('records')[0]
        selectedMappingID  = mappings.loc[st.session_state["masterList"]["selection"]["rows"]].to_dict('records')[0]["MAPPING_ID"]

# UI Layout and Components
# The sidebar contains the filter options for the user.
with st.sidebar:
   
    st.header("Filters")
    currentProject = st.selectbox("Projects",projectList,format_func=lambda x:x["NAME"],key="projectSelect")
    currentSystem = st.selectbox("Select a System",systemList,format_func=lambda x:x["NAME"],key="systemSelect")
    currentDatabase = st.selectbox("Select a Database", databaseList,format_func=lambda x:x["NAME"],key="databaseSelect")
    currentSchema = st.selectbox("Select a Schema",schemaList,format_func=lambda x:x["NAME"],key="schemaSelect")  
    currentTable = st.selectbox("Select a Table",tableList, format_func=lambda x:x["NAME"],key="tableSelect")
    st.button('Reset Filters',on_click=resetFilters)
       
st.header("EDA Custom Mapping")
masterList = st.session_state["masterList"]

# This expander shows the list of existing mappings based on the selected filters.
with st.expander('MAPPING LIST',expanded=not mappingSelected):
    with st.container(border=True):
        st.subheader('Existing Mappings');
        if currentTable:
            # Display the data in a Streamlit dataframe.
            if st.dataframe(mappings,
                        use_container_width=True,
                        hide_index=True,
                        on_select='rerun',
                        selection_mode=['single-row'],
                       
                        # Configure which columns to show and their properties.
                        column_config={
                        "MAPPING_ID": None,
                        "SOURCE_TABLE_ID":None,
                        "PROJECT_ID":None
                        },
                        key ='masterList'
            ):
                st.session_state["selectedRow"] = st.session_state["masterList"]["selection"] 
                #st.write(st.session_state["selectedRow"])
            
            # Button to create a new mapping if one is not already selected.
            if not mappingSelected:
                if st.button('Create New For Selected Source',key="newMapBtn"):
                    success = da.create_mapping_master(st.session_state["tableSelect"],st.session_state["projectSelect"],st.session_state["databaseSelect"],st.session_state["schemaSelect"])
                    st.write(success)
                    st.session_state["newMapping"] = True  
                    st.rerun()
                    
# This container holds the main tabs for mapping configuration.
with st.container(border=True,key="mainContainer"):
    
    # Check if a mapping is selected to display the detailed tabs.
    if mappingSelected:
        tabMain,tabColumn,tabValues,tabPreview = st.tabs(["Main","Column Mapping","Value Translation","Preview"])
        
        # Main Tab: Allows editing of the core mapping details.
        with tabMain:
            with st.form(key="mainForm"):
                # Display the selected mapping details in a clean DataFrame.
                st.dataframe(pd.DataFrame([selectedMapping]), 
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "ID": st.column_config.Column("ID", disabled=True),
                        "NAME": st.column_config.Column("Name", disabled=True),
                        "DESCRIPTION": st.column_config.Column("Description", disabled=True),
                        "DESCRITPTION": st.column_config.Column("Description", disabled=True)
                    }
                )

                targetDB = st.text_input("Target Database",selectedMapping["TARGETDB"])
                targetSchema = st.text_input("Target Schema",selectedMapping["TARGETSCHEMA"])
                targetObject = st.text_input("Target Object Name",selectedMapping["TARGETOBJECT"])
                targetObjectType = st.selectbox("Target Object Type",("View","Dynamic Table"),)
                trySave = st.form_submit_button("Save");
                if trySave:
                    success = da.save_mapping_master(selectedMappingID, targetDB, targetSchema, targetObject, targetObjectType)
                    if success:
                        st.rerun()
                   
        # Column Mapping Tab: Displays and allows editing of column-level mappings.
        with tabColumn:
            with st.form(key="ColumnForm"): 
                colEditor = st.data_editor(columnMappings,key='ed',num_rows="dynamic", use_container_width=True,
                                        column_order=("IsMapped","SourceColumn","TARGETCOLUMN","DESCRIPTION"),
                                        column_config={
                                        "MAPPING_COLUMN_ID": None,
                                        "TABLE_ID":None,
                                        "MAPPING_ID":None,
                                        "MAPPING_COLUMN_ID":None,
                                        "IsMapped":st.column_config.CheckboxColumn(label="Include",  width="small", help="Include this column in the target Model?"),
                                        "SourceColumn": st.column_config.Column(label="Source Column",disabled = True)
                                        },)
                
                # Buttons for previewing, saving, and canceling.
                col1, col2, col3 = st.columns(3)
    
                with col1:
                    preview = st.form_submit_button("Preview")
                    if preview:
                        sql = da.preview_sql(selectedMapping, colEditor, valueMappings)
                        st.subheader("Generated SQL")
                        st.code(sql, language="sql")

                with col2:
                    submitted = st.form_submit_button("Save")
                    if submitted:
                        da.save_columns(colEditor, selectedMappingID)
                        st.success("Column mappings saved successfully!")
    
                
                with col3:
                    cancelled = st.form_submit_button("Cancel")
                    if cancelled:
                        st.write("Cancel Logic")    
           
        # Value Translation Tab: Allows for value-level translations.
        with tabValues:
            
            # Filter available columns to those that are mapped.
            availableCols = colEditor.loc[lambda x: x['IsMapped']==True].to_dict('records')
            selectedColumnforValue = st.selectbox("Availble Columns",options = availableCols,key="colSelect",format_func=lambda x:x["SourceColumn"])
            valueMappings = da.get_mapped_values(st.session_state["colSelect"])
            #st.write(valueMappings)
            if selectedColumnforValue:
                #st.write(valueMappings)
                # Display the data editor for value mappings.
                st.data_editor(valueMappings,key="valueEd",num_rows='dynamic',column_config={
                                        "MAPPING_COLUMN_ID":None,
                                        },)
    # Message to the user if no mapping is selected.
    else:
        st.write('Please Select a mapping or create a new one')
