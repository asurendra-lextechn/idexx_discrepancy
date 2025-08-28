import os
from imbox import Imbox
import traceback
from dotenv import load_dotenv

def download_attachments():
    """
    Connects to an IMAP server, finds unread emails with a specific subject,
    and downloads their attachments.
    """
    load_dotenv()

    imap_server = os.getenv("IMAP_SERVER", "imap.gmail.com")
    imap_user = os.getenv("EMAIL_SENDER")
    imap_password = os.getenv("EMAIL_PASSWORD")
    download_folder = "./IDEXX DISCREPANCY EXCEL FILES"
    email_subject = "2025 VSA VSJ Discrepancies"

    if not os.path.isdir(download_folder):
        os.makedirs(download_folder)

    if not all([imap_server, imap_user, imap_password]):
        print("Error: IMAP_SERVER, IMAP_USER, or IMAP_PASSWORD not found in .env file.")
        return

    try:
        # Use a 'with' statement for safe connection handling (login/logout)
        # and explicitly enable SSL.
        with Imbox(hostname=imap_server,
                   username=imap_user,
                   password=imap_password,
                   port=993,
                   ssl=True) as mail:

            print(f"Successfully connected to {imap_server} as {imap_user}")
            messages = mail.messages(subject=email_subject, unread=True)
            print(f"Found {len(messages)} unread messages with subject '{email_subject}'")

            for uid, message in messages:
                mail.mark_seen(uid)
                for idx, attachment in enumerate(message.attachments):
                    try:
                        att_fn = attachment.get('filename')
                        if att_fn:
                            download_path = os.path.join(download_folder, att_fn)
                            print(f"Downloading attachment: {att_fn}")
                            with open(download_path, "wb") as fp:
                                fp.write(attachment.get('content').read())
                    except Exception:
                        print(f"Error processing attachment {att_fn}:")
                        traceback.print_exc()

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

    print("Email download process finished.")

if __name__ == "__main__":
    download_attachments()
