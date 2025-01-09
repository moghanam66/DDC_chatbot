import os
import sys
import base64
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from embedchain import App
from pyspark.sql import SparkSession
from fastapi.responses import JSONResponse
import uvicorn

# Initialize FastAPI app
api_app = FastAPI()

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
def initialize_app(config_path="config.yaml", metadata_file=["database_metadata5.csv", "V_PROM_COPR_NAVIS.csv",
                                                             'V_SITSPOOL_NAVIS.csv', 'V_PROM_NAVIS.csv', 'V_PROM_COPR_DELAYED.csv'], query_file="calculating_status.txt"):
    app = App.from_config(config_path=config_path)
    app.add(metadata_file[0])
    app.add(metadata_file[1])
    app.add(metadata_file[2])
    app.add(metadata_file[3])
    app.add(metadata_file[4])
    app.add(query_file, data_type='text')
    return app

app = initialize_app()

# Project mapping
project_mapping = {
    "HG": "HG_032320",
    "BONNY": "BONNY_514001",
    "PERDAMAN": "PERDAMAN_835057",
    "BERRI": "BERRI_H02585",
    "MARJAN": "MARJAN_H02586"
}

# API Endpoint for project selection
class ProjectSelection(BaseModel):
    project: str

@api_app.post("/select_project/")
async def select_project(project: ProjectSelection):
    if project.project in project_mapping:
        project_id = project_mapping[project.project]
        return {"message": f"Project '{project.project}' selected.", "project_id": project_id}
    else:
        raise HTTPException(status_code=400, detail="Invalid project name.")

# API Endpoint for generating SQL query
class QueryRequest(BaseModel):
    user_input: str
    project_id: str

@api_app.post("/generate_sql/")
async def generate_sql(request: QueryRequest):
    try:
        prompt = (
            f"Write only the Oracle SQL query to retrieve {request.user_input}. "
            f"Do not include any explanation, comments, or additional text. Just the Oracle SQL query. "
            f"Make sure that column and table names are from metadata. "
            f"Do not join two tables. "
            f"Foundation: %FND%"
        )
        response = app.query(prompt)
        query_start = response.find("SELECT")
        query_end = response.rfind(";")
        if ';' in response:
            return {"sql_query": response[query_start:query_end].replace("\n", " ").strip()}

        else:
            return {"sql_query": response[query_start:query_end+1].replace("\n", " ").strip()}
        
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# API Endpoint for executing SQL query
class ExecuteQueryRequest(BaseModel):
    query: str
    project_id: str

@api_app.post("/execute_query/")
async def execute_query(request: ExecuteQueryRequest):
    try:
        oracle_url = "jdbc:oracle:thin:@//sasv03h9.saipemnet.saipem.intranet:1521/sicon"
        oracle_properties = {
            "user": request.project_id,
            "password": request.project_id,
            "driver": "oracle.jdbc.OracleDriver"
        }
        query = f"({request.query}) alias"
        df = spark.read.jdbc(
            url=oracle_url,
            table=query,
            properties=oracle_properties
        ).limit(20)
        print(df)
        pandas_df = df.toPandas()
        return JSONResponse(content=pandas_df.to_dict(orient="records"))
    except Exception as e:
        error_message = str(e)
        print(error_message)
        if "ORA-00942" in error_message:
            raise HTTPException(status_code=404, detail="Table doesn't exist.")
        else:
            raise HTTPException(status_code=500, detail="An error occurred while fetching data.")

# Run the API server
if __name__ == "__main__":
    uvicorn.run(api_app, host="0.0.0.0", port=8000)
