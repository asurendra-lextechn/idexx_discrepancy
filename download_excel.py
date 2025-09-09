import os
import msal
import imaplib
import email
from email.header import decode_header
import traceback
from dotenv import load_dotenv

# --- Load Configuration from .env file ---
load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_SERVER = os.getenv("IMAP_SERVER", "outlook.office365.com")

# Define folder structure
BASE_DOWNLOAD_FOLDER = os.getenv("DOWNLOAD_FOLDER", "IDEXX Discrepancy files")
NEW_FOLDER = os.path.join(BASE_DOWNLOAD_FOLDER, "New")

SCOPE = ["https://outlook.office.com/.default"]

def get_access_token():
    """Acquires the full access token object using the client credentials flow."""
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" in result:
        print("Successfully acquired access token.")
        return result
    else:
        error_details = result.get('error_description', 'No error description provided.')
        raise Exception(f"Failed to get access token: {error_details}")

def generate_auth_string(user, token):
    """Generates the raw XOAUTH2 authentication string."""
    return f"user={user}\x01auth=Bearer {token}\x01\x01"

def download_attachments():
    """
    Connects to the mailbox, finds emails with 'discrepancy' in the subject,
    downloads attachments to the 'New' folder, and marks emails as read.
    """
    if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET, IMAP_USER]):
        print("Error: Ensure TENANT_ID, CLIENT_ID, CLIENT_SECRET, and IMAP_USER are in your .env file.")
        return

    # Create the 'New' directory if it doesn't exist
    os.makedirs(NEW_FOLDER, exist_ok=True)

    mail = None
    try:
        token_object = get_access_token()
        access_token = token_object['access_token']

        mail = imaplib.IMAP4_SSL(IMAP_SERVER, 993)
        mail.authenticate(
            "XOAUTH2",
            lambda x: generate_auth_string(IMAP_USER, access_token).encode()
        )
        print("Successfully authenticated with the mailbox.")

        mail.select("INBOX")
        # Search for all unread emails to filter them client-side
        status, messages = mail.search(None, '(UNSEEN)')
        if status != "OK":
            print("Error searching for messages.")
            return

        message_ids = messages[0].split()
        print(f"Found {len(message_ids)} unread messages. Now filtering by subject...")

        for msg_id in message_ids:
            # Fetch only the email header to check the subject
            status, msg_data = mail.fetch(msg_id, "(BODY[HEADER.FIELDS (SUBJECT)])")
            if status != "OK": continue

            subject_header = msg_data[0][1].decode('utf-8')
            subject = email.header.make_header(email.header.decode_header(subject_header.replace('Subject: ', ''))).__str__()

            # Case-insensitive check for 'discrepancy' or 'discrepancies'
            if any(word in subject.lower() for word in ['discrepancy', 'discrepancies']):
                print(f"  - Match found: Subject '{subject}'. Processing message ID {msg_id.decode()}.")

                # Now fetch the full email to get attachments
                status, full_msg_data = mail.fetch(msg_id, "(RFC822)")
                if status != "OK": continue

                msg = email.message_from_bytes(full_msg_data[0][1])
                downloaded_something = False
                for part in msg.walk():
                    if "attachment" in str(part.get("Content-Disposition", "")):
                        filename = part.get_filename()
                        if filename:
                            filepath = os.path.join(NEW_FOLDER, filename)
                            print(f"    - Downloading attachment: {filename}")
                            with open(filepath, "wb") as f:
                                f.write(part.get_payload(decode=True))
                            print(f"    - Saved to: {filepath}")
                            downloaded_something = True

                # Mark email as read ONLY if we successfully downloaded an attachment
                if downloaded_something:
                    mail.store(msg_id, '+FLAGS', '\\Seen')
                    print(f"    - Marked message ID {msg_id.decode()} as read.")
            else:
                # If subject doesn't match, we can optionally mark it as read to ignore next time
                # mail.store(msg_id, '+FLAGS', '\\Seen')
                pass

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
    finally:
        if mail:
            if mail.state == 'SELECTED':
                mail.close()
            mail.logout()
            print("Connection closed.")

    print("Email download process finished.")

if __name__ == "__main__":
    download_attachments()