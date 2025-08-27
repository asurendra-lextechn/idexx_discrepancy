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

def get_workorder_value():
    excel_file = "example_excel.xlsx"
    df = pd.read_excel(excel_file, sheet_name='8.1')
    vendor_bag_count = df['VENDOR BAG COUNT']
    lab_bag_count = df['LAB BAG COUNT']

    if vendor_bag_count < lab_bag_count:
        workorderid = df['WORKORDER']
        value = lab_bag_count
        tuple_value = (workorderid, value)
    return tuple_value


def update_db(workorder_value):
    """Execute parameterized updates for a list of (workorderid, value)."""
    if not workorder_value:
        return
    engine = get_sql_server_engine()
    sql = text("""
        UPDATE manifestlocationcolumnmappingvalue
           SET VALUE = :value
        FROM IdexxServiceBU i
        JOIN manifestlocationmappings mlm ON i.manifestlocationmappingid = mlm.id
        JOIN manifestlocationcolumnmappings mlcm ON mlcm.ManifestLocationMappingId = mlm.id
        JOIN manifestlocationcolumnmappingvalue mlcmv ON mlcmv.manifestlocationcolumnmappingid = mlcm.manifestlocationcolumnmappingid
       WHERE i.workorderid = :workorderid AND mlcm.ColumnId = 38
    """)
    try:
        with engine.begin() as conn:
            for workorderid, value in workorder_value:
                conn.execute(sql, {"workorderid": int(workorderid), "value": int(value)})
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

    if username and password:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
            server.login(username, password)
            server.sendmail(sender, recipient, msg.as_string())
    else:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.sendmail(sender, recipient, msg.as_string())

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
    vendor_col = _find_column(df, "VENDOR BAG COUNT")
    lab_col = _find_column(df, "LAB BAG COUNT")
    workorder_col = _find_column(df, "WORKORDER")
    notes_col = _find_column(df, "NOTES") or _find_column(df, "NOTE")

    if not (vendor_col and lab_col and workorder_col):
        raise ValueError("Required columns not found in sheet: VENDOR BAG COUNT, LAB BAG COUNT, WORKORDER")

    notes_series = df[notes_col].fillna("") if notes_col else pd.Series([""] * len(df), index=df.index)
    mask = (df[vendor_col].astype(float) < df[lab_col].astype(float)) & (notes_series.str.lower() != "updated")
    to_process = df.loc[mask]

    # prepare workorder/value tuples
    workorder_values = []
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
    smtp_port = int(os.getenv("SMTP_PORT", os.getenv("SMTP_PORT", 25)))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    sender = os.getenv("EMAIL_SENDER", "noreply@example.com")
    subject = "IDEXX Package count discrepancy"
    body = "All the VENDOR BAG COUNT< LAB BAG COUNT package counts are updated"

    send_email_with_attachment(smtp_server, smtp_port, sender, email_recipient, subject, body, file_path, smtp_user, smtp_pass)

if __name__ == "__main__":
    # quick run for local development
    example_file = os.path.join(os.path.dirname(__file__), "example_excel.xlsx")
    process_latest_excel(example_file)
