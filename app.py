import dash
from dash import html, dash_table
import plotly.express as px
import pandas as pd
import requests
import json
import time
import dash
from dash import Dash, dcc, html, Input, Output,callback
import dash_bootstrap_components as dbc
from dash import Input, Output, State, html
from dash_bootstrap_components._components.Container import Container
from azure.storage.blob import BlobServiceClient
from docx import Document
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks_api import DatabricksAPI
import time
import plotly.graph_objs as go
import base64
import io
import os
import zipfile

msg = " "
global x
x = 1
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME, dbc.icons.BOOTSTRAP],
    meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}]
)
FA_icon = html.I(className="fa-solid fa-cloud-arrow-up")
Da_icon = html.I(className="fa-solid fa-cloud-arrow-down")
# FA_button =  dbc.Button()
layout = html.Div([
html.Div([
html.H2(id='header',children="Athena Databricks Pipeline",style={'margin-left':'50%','padding-left':'60%','padding':'10px','margin-top':'-150px',
'font-size':'40px','border-width':'3px','border-color':'#a0a3a2','align':'center'
}),
 html.Br(),
 html.Br(),
html.Div(
    
[          
            html.H5("Company",style={'font-size':'25px','border-color':'#a0a3a2','align':'center','margin-left':'51%','margin-right':'21%','margin-top':'100px'}),
            dbc.Input(placeholder="Enter Company name...", id='company',type="text",style={'font-size':'15px','border-width':'5px','width': '15%','border-color':'#a0a3a2','align':'center','margin-left':'51%'}),
            html.H5("Client",style={'font-size':'25px','border-color':'#a0a3a2','align':'center','margin-left':'51%','margin-right':'21%','margin-top':'20px'}),
            dbc.Input(placeholder="Enter Client name...", id='client', type="text",style={'font-size':'15px','border-width':'5px','width': '15%','border-color':'#a0a3a2','align':'center','margin-left':'51%'}),
            html.H5("Reference Number",style={'font-size':'25px','border-color':'#a0a3a2','align':'center','margin-left':'51%','margin-right':'21%','margin-top':'20px'}),
            dbc.Input(placeholder="Enter Reference Number...", id='reference', type="text",style={'font-size':'15px','border-width':'5px','width': '15%','border-color':'#a0a3a2','align':'center','margin-left':'51%'}),
            html.H5("To Email ID ",style={'font-size':'25px','border-color':'#a0a3a2','align':'center','margin-left':'51%','margin-right':'21%','margin-top':'20px'}),
            dbc.Input(placeholder="Enter Recepient email id...", id='email_id', type="email",style={'font-size':'15px','border-width':'5px','width': '15%','border-color':'#a0a3a2','align':'center','margin-left':'51%'}),
            html.H5("Purpose",style={'font-size':'25px','border-color':'#a0a3a2','align':'center','margin-left':'51%','margin-right':'21%','margin-top':'20px'}),
            dcc.Dropdown(placeholder="Select an option...", id='purpose',className='custom-dropdown',  options=[
            {'label': 'Pre-sales', 'value': 'Pre-sales'},
            {'label': 'RFP', 'value': 'RFP'},
            {'label': 'RFI', 'value': 'RFI'},
            {'label': 'RFX', 'value': 'RFX'}
            ],style={'font-size':'15px','border-width':'5px','width': '35%','border-color':'#a0a3a2','align':'center','margin-left':'38%','margin-top':'20px'}),            
            html.H5("NDA Document",style={'font-size':'25px','border-width':'30px','border-color':'#a0a3a2','align':'center','margin-left':'51%','margin-top':'50px'}),
            dcc.Upload(
                id='upload-data',
                children=dbc.Button([FA_icon, " Upload"], className="me-1",size="lg", color="info",id='uploadfile', n_clicks=0, style={'border-width':'3px','font-size':'14px'}),
style={'margin-left':'52%','padding-top':'20px'},
# html.Button('Upload File', style= { 'height': '50px','textAlign': 'center',}),
#                 style={
#                     'width': '50%',
#                     'height': '50px',
#                     'lineHeight': '60px',
#                     'borderWidth': '1px',
#                     'borderStyle': 'dashed',
#                     'borderRadius': '5px',
#                     'textAlign': 'center',
#                     'margin': '10px',
#                     'margin-left':'58%',
#                     'padding-top':'20px'
#                 },
                
                multiple=False,
            ),
            html.Div(id='file-name-display', style={'margin-left':'58%','margin-top': '-25px','font-size':'3px'}),
        ]
,className='divBorder'
# style={'padding-left':'50%','padding-top':'10px','border-width':'3px','height':'45px'}
 ),
html.Br(),
html.Div(
    children=[
        ' ',
        html.H5(id='error_message',children='',style={'margin-left':'51%','width':'450px','height':'45px','margin-top':'4px',
'font-size':'16px','border-width':'3px','border-color':'#a0a3a2',"color": "red"
}),
    ]
),
html.Br(),
html.Br(),
html.Div(
dbc.Button('Start Pipeline',color="secondary",size="lg", className="me-1",id='submitval', n_clicks=0, style={'border-width':'3px','font-size':'14px'}),
style={'margin-left':'58%','padding-top':'20px'}),

dcc.Store(id='intermediate-value'),
html.Br(),
html.Div(
    id='output-3',
    children=[
        ' ',
        html.H5(id='output-4', children='',style={'margin-left':'35%','width':'450px','height':'45px','padding':'10px','margin-top':'10px',
'font-size':'16px','border-width':'3px','border-color':'#a0a3a2',
}),
    ]
),
dcc.Interval(
            id='interval-component',
            interval=3000,
            n_intervals=3
    ),
# html.Div(id='dd-output-container',style=dict(display='flex'))
],className='outside'),
dcc.Download(id="download-file")
])


# Blob storage and Databricks setup
blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=stcipdatasciencesandbox;AccountKey=qYWxwvIjME8Yo//UR3D9NJ4PDqETyKHZqaG2EwZA3IZrJbqnMPyHjuTo31U3IJNiBRaYDknUa5LK+AStXdoa+Q==;EndpointSuffix=core.windows.net')
container_name = 'athena'

databricks = DatabricksAPI(
    host="https://adb-7583395776102528.8.azuredatabricks.net/",
    token="dapi90e9e5f9c214f997576b515e4bcf839b-3"
)
w = WorkspaceClient(
    host="https://adb-7583395776102528.8.azuredatabricks.net/",
    token="dapi90e9e5f9c214f997576b515e4bcf839b-3"
)
cluster_id = "0521-102544-4qjmt2wf"


def wait_for_run_completion(databricks, run_id, timeout=1800, poll_interval=30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        run_status = databricks.jobs.get_run(run_id)
        if run_status['state']['life_cycle_state'] == 'TERMINATED':
            if run_status['state']['result_state'] == 'SUCCESS':
                print(run_status)
                return True
            else:
                raise Exception(f"Databricks job failed: {run_status['state']['state_message']}")
        time.sleep(poll_interval)
    raise TimeoutError("Databricks job did not complete within the specified timeout period")


# Callback to display the filename and content immediately after upload
@app.callback(
    Output('file-name-display', 'children'),
    [Input('upload-data', 'contents'),
     Input("client", "value")],
    [State('upload-data', 'filename')]
)
def display_file_info(contents,client,filename):
    if contents is not None:
        # Display the file name
        filename = client + "/" + filename
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        document = Document(io.BytesIO(decoded))
        
        # Save to Azure Blob Storage
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
        blob_client.upload_blob(decoded, overwrite=True)
        file_name_display = html.H5(f"Uploaded file: {filename}")
        # x = 1
        
        return file_name_display
    return ""


@app.callback(
    Output('output-4', 'children'),
    [
     Input("submitval", "n_clicks"),
     Input("client", "value"),
     Input('interval-component', 'n_intervals')],
     [State('upload-data', 'filename')]
)
def update_message(n_clicks,client,n_intervals,filename):
    # ctx = dash.callback_context
    # print(1)

    global msg
    if  msg=="cometed":
        msg="done"
        print(msg)
        return "Pipline exececution is completed."
    elif msg == "inProgress":
        print("Pipeline in progress...")
        return "Pipeline execution in progress..."
    
    elif msg == "completed":
        print("Pipeline completed.")
        
        # Assuming 'x' is used to indicate something related to the filename processing
        if filename:
            client_path = f"{client}/"
            generator = blob_service_client.get_container_client(container_name)
            my_blobs = generator.list_blobs()

            with zipfile.ZipFile(f"{client}.zip", mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
                for blob in my_blobs:
                    if blob.name.startswith(client_path):
                        print(f"Adding {blob.name} to zip.")
                        blob_bytes = generator.get_blob_client(blob.name).download_blob().readall()
                        zf.writestr(blob.name, blob_bytes)

            with open(f"{client}.zip", "rb") as zip_file:
                encoded_zip = base64.b64encode(zip_file.read()).decode()

            layout = html.Div([
                dbc.Button(
                    [Da_icon, " Download"], className="me-1", size="lg", color="info",
                    id='download-button', n_clicks=0
                ),
                dcc.Store(id='stored-processed-file', data={'content': encoded_zip, 'filename': f"{client}.zip"})
            ])
            return layout
    else :
        return "Start the pipeline"

    

# def message(x,y):
#     global tbl_message
#     tbl_message = f"Top {y} features for {x} variable"

@app.callback(
    Output('error_message', 'children'),
    [
     Input("submitval", "n_clicks"),
     Input("company", "value"),
     Input("client", "value"),
     Input("reference", "value"),
     Input("email_id", "value"),
     Input("purpose", "value"),
     Input('upload-data', 'contents')],
      [State('upload-data', 'filename')]
)
def update_output(n_clicks,Company,Client,Reference,email,purpose,File,filename):
    ctx = dash.callback_context
    global target
    global n
    # target = contents
    print(n_clicks)
    print(email)
    if Company == "":
        Company = None
    if Client == "":
        Client = None
    
    if "submitval"==ctx.triggered_id:
        print(Company)
        if (Company != None and Client != None and File != None):
            global msg
            msg = "inProgress"
            result = call('POST',Company,Client,Reference,email,purpose,filename)
            print(result)
            return ""
        elif (Company == None and Client == None ):
            return "Enter both Company and Client's name!"
        elif (Company == None ):
            return "Enter Company  name!"
        elif (Client == None ):
            return "Enter Client's name!"
        elif (Reference == None ):
            return "Enter Reference Number!"
        elif (File == None ):
            return "Upload NDA"
    else :
        return ""

def call(request,Company,Client,Reference,email,purpose,filename):
    # url = f"https://dev.azure.com/rathyinvichiliravi/SNS_Bank/_apis/pipelines/9/runs?api-version=7.1-preview.1"
    # print(url)
    global msg
    print(msg)
    global processed_filename
    state = "InProgres.."
    job_payload = {
        "run_name": "Word Document Processing",
        "existing_cluster_id": cluster_id,
        "job_id" : 105638172859751,
        
        "libraries": [],
        "notebook_task": {
            "notebook_path": "/Workspace/Users/rathyin.vichiliravi@concentrix.com/Athena/Athena_Compare_Add_Comments( FrontEnd_Dash)",
            "base_parameters": {"input_path": "athena/filename","company_name": Company,"client_name": Client,"Reference_number" : Reference,"Email_Id" : email}
        }
    }
    input_filename = Client + "/" + filename
    run_id = w.jobs.run_now(job_id=105638172859751,job_parameters = {"input_path":input_filename,"nda_purpose" :purpose,  "company_name": Company,"client_name": Client,"Reference_number" : Reference,"Email_Id" : email}).result()
    # run_id = databricks.jobs.submit_run(**job_payload)['run_id']
    print(run_id)
    run_id=run_id.tasks[0].run_id
    # Wait for the job to complete
    # status = wait_for_run_completion(databricks, run_id)
    # print(status)
    output = databricks.jobs.get_run_output(run_id)
    print(output)
    processed_filename = 'Yeti/CNDACF0704238EN-Yeti-CNXC US1(UNDA)13-08-2024redRL-v1.docx'
    msg="completed" 
    return("Pipline exececution is completed.")
    
@app.callback(
    Output("download-file", "data"),
    Input("download-button", "n_clicks"),
    State('stored-processed-file', 'data'),
    suppress_callback_exceptions=True,
    prevent_initial_call=True
)
def download_file(n_clicks, data):
    if n_clicks > 0:
        global msg
        global x
        msg = 'finished'
        x = 2
        return dcc.send_bytes(base64.b64decode(data['content']), data['filename'])
    

    

sidebar = html.Div(
    [
        html.Div(
            [
                html.H3("Athena Demo", style={"color": "#a3a39d"}),
            ],
            className="sidebar-header",
        ),
        html.Hr(),
        html.Div(
            [
        dbc.Nav(
            [
                dbc.NavLink(
                    [html.I(className="fas fa-home me-2"), html.Span("Dashboard")],
                    href="/",
                    active="exact",
                ),
            ],
            vertical=True,
            pills=True,
        ) ],
            className="sidebar-header",
        ),
    ],
    className="sidebar",
)

app.layout = html.Div(
    [
        sidebar,
       layout
    ]
)

if __name__ == "__main__":
    app.run_server(debug=True)
