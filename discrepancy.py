import os
import pandas as pd
import logging
import traceback
import smtplib
import ssl
import shutil
import openpyxl
from urllib.parse import quote_plus
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Import the download function from the other script
import download_excel


# --- Load Configuration and Define Paths ---
load_dotenv()
BASE_FOLDER = "IDEXX Discrepancy files"
NEW_FOLDER = os.path.join(BASE_FOLDER, "New")
COMPLETED_FOLDER = os.path.join(BASE_FOLDER, "Completed")
ERROR_FOLDER = os.path.join(BASE_FOLDER, "Error")

def get_sql_server_engine():
    """Create SQLAlchemy engine for SQL Server"""
    server = os.getenv('server_name', '10.120.17.99')
    database = os.getenv('database_name')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    driver = os.getenv('ODBC_DRIVER', 'ODBC Driver 17 for SQL Server')
    
    username_encoded = quote_plus(username) if username else ""
    password_encoded = quote_plus(password) if password else ""
    connection_string = f"mssql+pyodbc://{username_encoded}:{password_encoded}@{server}/{database}?driver={driver}&Encrypt=no&TrustServerCertificate=yes&timeout=30"
    engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=300)
    return engine

def update_db(workorder_value):
    """
    Execute parameterized updates for a list of (workorderid, value).
    Returns a list of workorder IDs that were successfully updated.
    """
    if not workorder_value:
        logging.info("No workorders to update in the database.")
        return []
    
    successfully_updated_ids = []
    engine = get_sql_server_engine()
    sql = text("""
        UPDATE mlcmv
           SET VALUE = :value, UpdatedOn = GETDATE(), UpdatedBy = '13F6B7B1-A934-4019-B97C-2FBC493CFDF3'
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
                if result.rowcount > 0:
                    logging.info(f"{result.rowcount} row(s) updated for workorderid: {workorderid}.")
                    successfully_updated_ids.append(workorderid)
                else:
                    logging.warning(f"No rows updated for workorderid: {workorderid}. Check if it exists.")
    except Exception:
        logging.error("DB update failed:\n%s", traceback.format_exc())
        raise
    
    return successfully_updated_ids

def _find_column(df, name):
    """Case-insensitive column lookup, returns first match or None."""
    lname = name.lower()
    for c in df.columns:
        if c.lower() == lname:
            return c
    return None

def send_email_with_attachment(smtp_server, smtp_port, sender, recipient, subject, body, attachment_path, username=None, password=None):
    """Send email with the attachment."""
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    with open(attachment_path, "rb") as f:
        part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
    msg.attach(part)

    if smtp_server and smtp_server.lower() != 'localhost' and username and password:
        context = ssl.create_default_context()
        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls(context=context)
                server.login(username, password)
                server.sendmail(sender, recipient, msg.as_string())
                logging.info(f"Email sent successfully to {recipient}")
        except Exception as e:
            logging.error("Failed to send email: %s", e)
            raise
    else:
        logging.warning("Skipping email: SMTP server/credentials not configured.")

def process_excel_file(file_path):
    """
    Processes a single Excel file: reads it, updates the DB, updates the Excel file,
    and sends a notification email with statistics.
    """
    logging.info(f"Processing file: {file_path}")
    xls = None  # Define xls here to ensure it's accessible in the finally block
    try:
        xls = pd.ExcelFile(file_path)
        latest_sheet_name = xls.sheet_names[-1]

        # --- Dynamic Header Finding Logic ---
        # Read the latest sheet without a header to find the real one
        df_temp = pd.read_excel(xls, sheet_name=latest_sheet_name, header=None)
        header_row_index = -1
        # Search for the header row in the first 10 rows of the sheet
        for i in range(min(10, len(df_temp))):
            # Check if the essential columns exist in this row (case-insensitive)
            row_values = [str(v).lower().strip() for v in df_temp.iloc[i].values]
            if "vendor bag count" in row_values and "lab bag count" in row_values and "workorder" in row_values:
                header_row_index = i
                break

        if header_row_index == -1:
            logging.error(f"Could not find header row in sheet '{latest_sheet_name}'. Searched first 10 rows.")
            raise ValueError("Required columns not found in sheet: VENDOR BAG COUNT, LAB BAG COUNT, WORKORDER")

        logging.info(f"Dynamically found header for sheet '{latest_sheet_name}' at row index {header_row_index}.")

        # --- Read all sheets, applying the found header only to the latest one ---
        sheets = {}
        for sheet_name in xls.sheet_names:
            header_to_use = header_row_index if sheet_name == latest_sheet_name else 0
            sheets[sheet_name] = pd.read_excel(xls, sheet_name=sheet_name, header=header_to_use)

        df = sheets[latest_sheet_name]

        # --- Clean column names by stripping leading/trailing whitespace ---
        df.columns = df.columns.str.strip()

        logging.info(f"Columns successfully read and cleaned: {df.columns.tolist()}")

        vendor_col = _find_column(df, "VENDOR BAG COUNT")
        lab_col = _find_column(df, "LAB BAG COUNT")
        workorder_col = _find_column(df, "WORKORDER")
        notes_col = _find_column(df, "NOTES") or _find_column(df, "NOTE")

        if not (vendor_col and lab_col and workorder_col):
            raise ValueError("Required columns not found in sheet: VENDOR BAG COUNT, LAB BAG COUNT, WORKORDER")

        # --- Calculate Statistics ---
        total_workorders = df[workorder_col].dropna().count()

        notes_series = df[notes_col].fillna("") if notes_col else pd.Series([""] * len(df), index=df.index)
        # Ensure numeric columns are treated as numbers, coercing errors to NaN
        df[vendor_col] = pd.to_numeric(df[vendor_col], errors='coerce')
        df[lab_col] = pd.to_numeric(df[lab_col], errors='coerce')

        mask = (df[vendor_col] < df[lab_col]) & (notes_series.str.lower() != "updated")
        to_process = df.loc[mask]
        
        discrepancy_count = len(to_process)
        successful_updates_count = 0

        workorder_values = []
        if to_process.empty:
            logging.info("No rows found in Excel that require processing.")
        else:
            logging.info(f"Found {len(to_process)} rows to process in Excel.")
            for _, row in to_process.iterrows():
                w = row[workorder_col]
                v = row[lab_col]
                try:
                    workorder_values.append((int(w), int(v)))
                except (ValueError, TypeError):
                    logging.warning(f"Skipping row with non-integer workorder/value: {w} / {v}")

        if workorder_values:
            successfully_updated_ids = update_db(workorder_values)
            successful_updates_count = len(successfully_updated_ids)

            if successfully_updated_ids:
                # Create a new mask to update only the rows that were successful in the DB
                success_mask = df[workorder_col].isin(successfully_updated_ids)
                
                if notes_col:
                    df.loc[success_mask, notes_col] = "UPDATED"
                else:
                    df["NOTES"] = ""
                    df.loc[success_mask, "NOTES"] = "UPDATED"
                sheets[latest_sheet_name] = df
            else:
                logging.warning("No database rows were updated, so the Excel file will not be modified.")


            # Explicitly close the reader before writing
            xls.close()
            xls = None

            with pd.ExcelWriter(file_path, engine="openpyxl", mode="w") as writer:
                for name, sheet_df in sheets.items():
                    sheet_df.to_excel(writer, sheet_name=name, index=False)
            logging.info(f"Successfully updated Excel file: {file_path}")

            # --- Set the last sheet as the active sheet ---
            try:
                workbook = openpyxl.load_workbook(file_path)
                # Set the active sheet to the last one in the workbook
                workbook.active = len(workbook.sheetnames) - 1
                workbook.save(file_path)
                logging.info(f"Set active sheet to '{latest_sheet_name}'.")
            except Exception as e:
                # This is a non-critical step, so we just log a warning if it fails
                logging.warning(f"Could not set active sheet. Error: {e}")

        smtp_server = os.getenv("SMTP_SERVER")
        smtp_port = int(os.getenv("SMTP_PORT", 587))
        smtp_user = os.getenv("EMAIL_SENDER")
        smtp_pass = os.getenv("EMAIL_PASSWORD")
        sender = os.getenv("EMAIL_SENDER", "noreply@example.com")
        email_recipient = os.getenv("EMAIL_RECIPIENT")
        
        # --- Create Dynamic Email Subject and Body ---
        # Remove .xlsx or .xls extension from file name for subject
        base_filename = os.path.splitext(os.path.basename(file_path))[0]
        subject = f"IDEXX Discrepancy Report Processed: {base_filename} {latest_sheet_name}"
        body = (
            f"<html><body>"
            f"<h2>Automatic Discrepancy Update Report</h2>"
            f"<ul>"
            f"<li>Total Workorders in Sheet: {total_workorders}</li>"
            f"<li>Workorders with Discrepancy (Vendor &lt; Lab): {discrepancy_count}</li>"
            f"<li>Successfully Updated in Database: {successful_updates_count}</li>"
            f"<li><b>Remaining Discrepancies (not updated): {total_workorders - successful_updates_count}</b></li>"
            f"</ul>"
            f"<p>This is an automated report.</p>"
            f"</body></html>"
        )

        send_email_with_attachment(smtp_server, smtp_port, sender, email_recipient, subject, body, file_path, smtp_user, smtp_pass)
    finally:
        # This block ensures the Excel file handle is closed even if errors occur
        if xls:
            xls.close()
            logging.info("Excel file handle closed.")

def process_new_files():
    """
    Main orchestration function. Finds new files, processes them, and moves them.
    """
    # Ensure all necessary folders exist
    for folder in [NEW_FOLDER, COMPLETED_FOLDER, ERROR_FOLDER]:
        os.makedirs(folder, exist_ok=True)

    logging.info("Starting workflow. Looking for new files...")
    files_to_process = [f for f in os.listdir(NEW_FOLDER) if f.endswith(('.xlsx', '.xls'))]

    if not files_to_process:
        logging.info("No new files to process.")
        return

    for filename in files_to_process:
        source_path = os.path.join(NEW_FOLDER, filename)
        try:
            process_excel_file(source_path)
            # If successful, move to 'Completed' folder
            destination_path = os.path.join(COMPLETED_FOLDER, filename)
            shutil.move(source_path, destination_path)
            logging.info(f"Successfully processed and moved '{filename}' to Completed folder.")
        except Exception as e:
            logging.error(f"Failed to process file '{filename}': {e}")
            traceback.print_exc()
            # If failed, move to 'Error' folder
            destination_path = os.path.join(ERROR_FOLDER, filename)
            shutil.move(source_path, destination_path)
            logging.warning(f"Moved failed file '{filename}' to Error folder.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("--- Starting Automated Workflow ---")
    
    # Step 1: Download new files from email
    logging.info("Step 1: Checking for new emails and downloading attachments...")
    try:
        download_status = download_excel.download_attachments()
        
        # Step 2: Process downloaded files only if the download was successful
        if download_status == 1:
            logging.info("Download successful. Proceeding to process files.")
            process_new_files()
        else:
            logging.info("No new files were downloaded. Checking for existing files to process...")
            # Still process any files that might be in the 'New' folder from a previous failed run
            process_new_files()
            
    except Exception as e:
        logging.error("A critical error occurred in the download step: %s", e)
        traceback.print_exc()

    logging.info("--- Workflow Finished ---")