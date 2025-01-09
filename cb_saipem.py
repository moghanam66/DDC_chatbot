import streamlit as st
import os
from embedchain import App
from pyspark.sql import SparkSession
import sys
import base64

# Function to set background from a local image
def set_background(image_path):
    # Encode the image to base64
    with open(image_path, "rb") as img_file:
        encoded_string = base64.b64encode(img_file.read()).decode()

    # Inject the base64 image as a background
    background_style = f"""
    <style>
    body {{
        background-image: url("data:image/png;base64,{encoded_string}");
        background-size: cover;
        background-repeat: no-repeat;
        background-attachment: fixed;
        background-position: center;
    }}
    </style>
    """
    st.markdown(background_style, unsafe_allow_html=True)

# Set the background (replace 'your-image.png' with the path to your local image)
set_background("saipem_cover.jpg")

# Set environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["GOOGLE_API_KEY"] = "AIzaSyAbIsEbh9FFECX0UUrOkDadM1fYIdn8cf8"

# Initialize Spark session
def initialize_spark(app_name="OracleConnector", jar_path="ojdbc11.jar"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", jar_path)
        .getOrCreate()
    )

spark = initialize_spark()

# Load EmbedChain App
def initialize_app(config_path="config.yaml", metadata_file=["database_metadata5.csv" , "V_PROM_COPR_NAVIS.csv" ,
                                                             'V_SITSPOOL_NAVIS.csv' , 'V_PROM_NAVIS.csv' , 'V_PROM_COPR_DELAYED.csv'] , query_file = "calculating_status.txt"):
    app = App.from_config(config_path=config_path)
    app.add(metadata_file[0])
    app.add(metadata_file[1])
    app.add(metadata_file[2])
    app.add(metadata_file[3])
    app.add(metadata_file[4])
    app.add(query_file , data_type='text')
    return app

app = initialize_app()

# Streamlit App UI
st.title("Interactive LLM Response App")
st.write("Click a button to select a project, then provide your query input.")

# Project Selection
project_mapping = {
    "HG": "HG_032320",
    "BONNY": "BONNY_514001",
    "PERDAMAN": "PERDAMAN_835057",
    "BERRI": "BERRI_H02585",
    "MARJAN": "MARJAN_H02586"
}

# Initialize variables
project_id = None
selected_project = None

# Project Buttons
for project, id in project_mapping.items():
    if st.button(project):
        project_id = id
        selected_project = project
        st.success(f"Project '{project}' selected.")

# User Input
user_input = st.text_input("Insert the required data from the database")

# Query Generation and Execution
if project_id and user_input:
    st.write(f"Selected Project: {selected_project}")
    st.write("You entered:", user_input)

    # Generate SQL Query
    def generate_sql_query(user_input):
        prompt = f"Write only the Oracle SQL query to retrieve {user_input}. " \
                 f"Do not include any explanation, comments, or additional text. Just the Oracle SQL query." \
                 f"make sure that column and table names are from metadata. " \
                 f"Do not join two tables" 
                #  f"if I am asking for delayed items search with STATUS delayed if not do not use delay" \
                #     f"use V_PROM_COPR_DELAYED table just when i ask for delayed items"
        response = app.query(prompt)
        st.write(response)

        query_start = response.find("SELECT")
        query_end = response.rfind(";")

        # if query_start == -1 or query_end == -1:
        #     st.error("Failed to generate a valid SQL query. Please refine your input.")
        #     return None
        if ';' in response:
            return response[query_start:query_end].replace("\n", " ").strip()

        else:
            return response[query_start:query_end+1].replace("\n", " ").strip()
            

    query = generate_sql_query(user_input)

    if query:
        st.write(query)

        # Oracle Database Configuration
        oracle_url = "jdbc:oracle:thin:@//sasv03h9.saipemnet.saipem.intranet:1521/sicon"
        oracle_properties = {
            "user": project_id,
            "password": project_id,
            "driver": "oracle.jdbc.OracleDriver"
        }

        # Execute Query and Display Results
        try:
            query = f"({query}) alias"
            st.write(query)
            df = spark.read.jdbc(
                url=oracle_url,
                table=query,
                properties=oracle_properties
            ).limit(20)

            pandas_df = df.toPandas()
            st.dataframe(pandas_df)
        except Exception as e:
            error_message = str(e)
            if "ORA-00942" in error_message:
                st.error("Table doesn't exist.")
            else:
                st.error(f"An error occurred while fetching data: {error_message}")
else:
    if not project_id:
        st.warning("Please select a project by clicking one of the buttons.")
    if not user_input:
        st.warning("Please provide input for the query.")
