Mapping Accelerator Mockup
This is a mockup of a Streamlit application built for a Snowflake environment. It's not a production-ready solution, but an example to demonstrate how you can create data mappings. You'll need to tailor it to your specific environment and requirements.

How to Deploy: Follow these two steps to get the mockup running in your Snowflake account.

1. Run the Deployment Script
The deployment_script.sql file sets up all the necessary database objects. You must run this script in a Snowflake SQL worksheet before using the app. This creates the EDACONFIG schema, tables, views, and stored procedures that the application depends on.

2. Create the Streamlit App
In your Snowflake account, create a new Streamlit application. Copy the code from streamlit_app.py into the main app file and import the other files: data_access.py and environment.yml. The data_access.py file is critical as it handles all the interactions with the database.

Once you've done this, the application will be ready for you to use.
