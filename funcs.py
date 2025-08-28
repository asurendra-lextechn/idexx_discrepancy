import pyodbc
import sys
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import date,datetime,timedelta
import logging
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib
import ssl




def connect_to_sql_server():
    """Connect to SQL Server using pyodbc with explicit driver & port."""
    load_dotenv()  
    driver   = os.getenv('ODBC_DRIVER', 'ODBC Driver 17 for SQL Server')
    server = os.getenv('server_name', 'localhost')
    port     = os.getenv('SQL_PORT', '1433')
    database = os.getenv('database_name')
    uid      = os.getenv('DB_USER')
    pwd      = os.getenv('DB_PASSWORD')

    conn_str = (
        f"Driver={{{driver}}};"
        f"Server={server},{port};"
        f"Database={database};"
        f"UID={uid};"
        f"PWD={pwd};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

def get_sql_server_engine():
    """Create SQLAlchemy engine for SQL Server"""
    load_dotenv()
    server = os.getenv('server_name', 'localhost')
    database = os.getenv('database_name')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    driver = os.getenv('ODBC_DRIVER', 'ODBC Driver 17 for SQL Server').replace(' ', '+')
    
    # URL encode credentials
    username_encoded = quote_plus(username) if username else ""
    password_encoded = quote_plus(password) if password else ""
    
    connection_string = f"mssql+pyodbc://{username_encoded}:{password_encoded}@{server}/{database}?driver={driver}&Encrypt=no&TrustServerCertificate=yes&timeout=30"
    
    engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=300)
    return engine 

def update_db(workorder_value):
    """Execute parameterized updates for a list of (workorderid, value)."""
    if not workorder_value:
        logging.info("No workorders to update.")
        return
    engine = get_sql_server_engine()
    sql = text("""
        UPDATE mlcmv
           SET VALUE = :value
          FROM IdexxService i
          JOIN manifestlocationmappings mlm ON i.manifestlocationmappingid = mlm.id
          JOIN manifestlocationcolumnmappings mlcm ON mlcm.ManifestLocationMappingId = mlm.id
          JOIN manifestlocationcolumnmappingvalue mlcmv ON mlcmv.manifestlocationcolumnmappingid = mlcm.manifestlocationcolumnmappingid
         WHERE i.workorderid = :workorderid AND mlcm.ColumnId = 38
    """)
    try:
        with engine.begin() as conn:
            for workorderid, value in workorder_value:
                logging.info(f"Executing update for workorderid: {workorderid} with value: {value}")
                result = conn.execute(sql, {"workorderid": int(workorderid), "value": int(value)})
                if result.rowcount == 0:
                    logging.warning(f"No rows updated for workorderid: {workorderid}. Check if it exists and meets query conditions.")
                else:
                    logging.info(f"{result.rowcount} row(s) updated for workorderid: {workorderid}.")
    except Exception:
        logging.error("DB update failed:\n%s", traceback.format_exc())
        raise

def _find_column(df, name):
    """Case-insensitive column lookup, returns first match or None."""
    lname = name.lower()
    for c in df.columns:
        if c.lower() == lname:
            return c
    return None



def send_email_with_attachment(smtp_server, smtp_port, sender, recipient, subject, body, attachment_path, username=None, password=None):
    """Send email with the attachment. If username/password are not provided, attempts plaintext SMTP (localhost)."""
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with open(attachment_path, "rb") as f:
        part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
    msg.attach(part)

    # Only attempt to send if a server other than localhost is specified, and we have credentials.
    if smtp_server and smtp_server.lower() != 'localhost' and username and password:
        context = ssl.create_default_context()
        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls(context=context)  # Secure the connection
                server.login(username, password)
                server.sendmail(sender, recipient, msg.as_string())
                print(f"Email sent successfully to {recipient}")
        except Exception as e:
            logging.error("Failed to send email: %s", e)
            raise
    else:
        logging.warning("Skipping email: SMTP server/credentials not configured.")

def process_latest_excel(file_path, email_recipient="asurendra@lextechn.com"):
    """
    1. Reads the latest sheet from the Excel file.
    2. Finds rows where VENDOR BAG COUNT < LAB BAG COUNT and NOTES != 'updated'.
    3. Calls update_db on those workorders.
    4. Marks NOTES='updated' on processed rows and writes back the Excel.
    5. Emails the updated Excel to email_recipient.
    """
    # load all sheets
    xls = pd.ExcelFile(file_path)
    latest_sheet = xls.sheet_names[-1]
    sheets = pd.read_excel(file_path, sheet_name=None)  # dict of all sheets
    df = sheets[latest_sheet]

    # find columns (case-insensitive)
    vendor_col = _find_column(df, 'VENDOR BAG COUNT')
    lab_col = _find_column(df, 'LAB BAG COUNT')
    workorder_col = _find_column(df, 'WORKORDER ')
    notes_col = _find_column(df, "NOTES") or _find_column(df, "NOTE")
    print(df.columns)
    if not (vendor_col and lab_col and workorder_col):
        raise ValueError("Required columns not found in sheet: VENDOR BAG COUNT, LAB BAG COUNT, WORKORDER")

    notes_series = df[notes_col].fillna("") if notes_col else pd.Series([""] * len(df), index=df.index)
    mask = (df[vendor_col].astype(float) < df[lab_col].astype(float)) & (notes_series.str.lower() != "updated")
    to_process = df.loc[mask]

    # prepare workorder/value tuples
    workorder_values = []
    if to_process.empty:
        logging.info("No rows found in Excel that require processing.")
    else:
        logging.info(f"Found {len(to_process)} rows to process in Excel:")
        for index, row in to_process.iterrows():
            logging.info(f"  - Row {index}: WORKORDER={row[workorder_col]}, VENDOR BAG COUNT={row[vendor_col]}, LAB BAG COUNT={row[lab_col]}")

    for _, row in to_process.iterrows():
        w = row[workorder_col]
        v = row[lab_col]
        try:
            workorder_values.append((int(w), int(v)))
        except Exception:
            logging.warning("Skipping row with non-integer workorder/value: %s / %s", w, v)

    # update DB
    if workorder_values:
        update_db(workorder_values)

        # mark updated in dataframe
        if notes_col:
            df.loc[mask, notes_col] = "updated"
        else:
            # create NOTES column if missing
            df["NOTES"] = ""
            df.loc[mask, "NOTES"] = "updated"
            sheets[latest_sheet] = df

        # write back all sheets (preserve others)
        with pd.ExcelWriter(file_path, engine="openpyxl", mode="w") as writer:
            for name, sheet_df in sheets.items():
                if name == latest_sheet:
                    sheet_df = df
                sheet_df.to_excel(writer, sheet_name=name, index=False)

    # send email (SMTP config from env)
    smtp_server = os.getenv("SMTP_SERVER", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    smtp_user = os.getenv("EMAIL_SENDER")
    smtp_pass = os.getenv("EMAIL_PASSWORD")
    sender = os.getenv("EMAIL_SENDER", "noreply@example.com")
    subject = "IDEXX Package count discrepancy"
    body = "All the VENDOR BAG COUNT< LAB BAG COUNT package counts are updated"

    send_email_with_attachment(smtp_server, smtp_port, sender, email_recipient, subject, body, file_path, smtp_user, smtp_pass)

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Load environment variables from .env file at the start
    load_dotenv()
    # quick run for local development
    example_file = os.path.join(os.path.dirname(__file__), "example_excel.xlsx")
    process_latest_excel(example_file)
