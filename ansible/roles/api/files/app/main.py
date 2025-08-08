from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse
import boto3
import pandas as pd
from io import StringIO
import uuid
import os
from dotenv import load_dotenv
load_dotenv()


app = FastAPI()

# S3 Config (make sure to set these in your environment securely)
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_REGION = os.getenv("AWS_REGION", "us-east-1")

def upload_to_s3(csv_data: str, filename: str):
    s3 = boto3.client('s3', aws_access_key_id=S3_KEY,
                      aws_secret_access_key=S3_SECRET,
                      region_name=S3_REGION)

    s3.put_object(Bucket=S3_BUCKET, Key=filename, Body=csv_data)
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=filename, Body=csv_data)
    except Exception as e:
        print("S3 Upload Error:", e)
        return {"error": str(e)}


@app.post("/convert")
async def convert_text_to_csv(text: str = Form(...)):
    try:
        # Example: Parse text to list of dicts
        lines = text.strip().split("\n")
        headers = lines[0].split(",")
        data = [dict(zip(headers, line.split(","))) for line in lines[1:]]
        print("Parsed Data:", data)

        # Convert to CSV
        df = pd.DataFrame(data)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        print("CSV Content:\n", csv_buffer.getvalue())
        # Upload to S3
        filename = f"{uuid.uuid4()}.csv"
        upload_to_s3(csv_buffer.getvalue(), filename)
        return {"message": "CSV uploaded to S3"}

        return JSONResponse(status_code=200, content={"message": "CSV uploaded", "filename": filename})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


#curl -X POST http://localhost:8000/convert  -F "text=name,age,city\nAlice,30,New York\nBob,25,London"