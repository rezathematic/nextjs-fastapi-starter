from fastapi import FastAPI, UploadFile, File
import pandas as pd
from io import StringIO

app = FastAPI()

def process_crawl_overview(data: pd.DataFrame) -> dict:
    # Identifying the blank lines to split the dataframes
    blank_lines = data.index[data.isnull().all(1)].tolist()[1:]
    dataframes = []
    start = 0
    for end in blank_lines:
        df = data[start:end]
        if not df.empty and not df.isnull().all().all():
            df = df.dropna(how="all")  # Exclude rows that are entirely NaN
            dataframes.append(df)
        start = end + 1
    dataframes.append(data[start:])  # Adding the last section

    # Converting to custom JSON structure
    json_dataframes_custom = {}

    for df in dataframes:
        section_title = df.iloc[0, 0]
        section_data = []
        for _, row in df.iloc[1:].iterrows():
            formatted_row = {
                "title": row[0] if pd.notna(row[0]) else "",
                "number of URLs": row[1] if pd.notna(row[1]) else "",
                "percentage": row[2] if pd.notna(row[2]) else ""
            }
            section_data.append(formatted_row)
        json_dataframes_custom[section_title] = section_data

    return json_dataframes_custom

def process_issues_overview(data: pd.DataFrame) -> dict:
    data["issue name"] = data["Issue Name"].apply(
        lambda x: x.split(":")[1].strip() if ":" in x else x
    )
    data["issue tag"] = data["Issue Name"].apply(
        lambda x: x.split(":")[0].strip() if ":" in x else "General"
    )
    data["issue priority"] = data["Issue Priority"]
    data = data[["issue name", "issue tag", "issue priority"]]
    data.columns = ["issue", "tag", "priority"]
    priority_mapping = {"Low": 3, "Medium": 2, "High": 1}
    data["priority_sort"] = data["priority"].map(priority_mapping)
    data_sorted = data.sort_values("priority_sort").drop("priority_sort", axis=1)
    data_sorted = data_sorted.head(15)

    # Converting the processed data to a dictionary
    issues_records = data_sorted.to_dict("records")
    return issues_records

@app.post("/api/process-csv")
async def process_csv(crawl_overview: UploadFile = File(...), issues_overview: UploadFile = File(...)):
    crawl_data = await crawl_overview.read()
    issues_data = await issues_overview.read()

    # Convert the uploaded files to DataFrames
    df_crawl_overview = pd.read_csv(StringIO(crawl_data.decode()))
    df_issues_overview = pd.read_csv(StringIO(issues_data.decode()))

    # Process the data
    crawl_json = process_crawl_overview(df_crawl_overview)
    issues_json = process_issues_overview(df_issues_overview)

    # Combine the results
    result = {
        "crawl_overview": crawl_json,
        "issues_overview": issues_json
    }

    return result

@app.get("/api/python")
def hello_world():
    return {"message": "Hello World"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
