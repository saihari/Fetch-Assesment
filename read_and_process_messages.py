from math import ceil
from sqlalchemy import create_engine
from datetime import date
from hashlib import sha3_512
import psycopg2
import multiprocessing
import traceback
import pandas as pd
import json
import boto3
import os


# Static Variables
QUEUE_URL = "http://localhost:4566/000000000000/login-queue"
QUEUE_END_POINT = "http://localhost:4566"
AWS_REGION = "us-east-1"
DATABASE = "postgres"
USER = "postgres"
PASSWORD = "postgres"
HOST = "localhost"
PORT = "5432"
ALL_COLUMNS = [
    "user_id",
    "device_type",
    "masked_ip",
    "masked_device_id",
    "locale",
    "app_version",
    "create_date",
]


def get_available_cpus() -> int:
    """
    Get number of CPUs

    Returns:
        int: # of CPU's available
    """
    try:
        num_cpus = os.cpu_count()
    except Exception as e:
        print("Exception Occured: ", e)
        num_cpus = 1  # if unable to find number of cpus a default of 1 is assigned

    return num_cpus


def process_data(data: list) -> None:
    """
    Mask PII data pulled from sqs queue and push it to postgres DB

    Args:
        data (list): list of dictionary objects containing the body of the message pulled from sqs queue.

    Raises:
        Exception: Unable to Connect to DB, if script could not establish connection to postgres db
        Exception: Error occurred in updating user_logins, if script could not push records to postgres db
    """
    print(f"Process {os.getpid()} Started")
    df = []  # List to store each processed message

    for message in data:
        # Converting message in str to json
        message = json.loads(message)

        # Masking ip
        if message.get("ip"):
            message["masked_ip"] = sha3_512(message["ip"].encode()).hexdigest()

            del message["ip"]  # Deleting unmasked ip

        # Masking device_id
        if message.get("device_id"):
            message["masked_device_id"] = sha3_512(
                message["device_id"].encode()
            ).hexdigest()

            del message["device_id"]  # Deleting device_id

        # Convert app_version into int
        if message.get("app_version"):
            message["app_version"] = int(message.get("app_version")[0])

        # Adding create_date
        message["create_date"] = date.today()

        # Ensuring all columns are present in the message
        for each in ALL_COLUMNS:
            if not message.get(each):
                message[each] = None

        df.append(message)

    df = pd.DataFrame.from_dict(df)

    # Ensuring extra columns are not present
    extra_columns = [each for each in df.columns if each not in ALL_COLUMNS]
    if extra_columns:
        df.drop(extra_columns, axis=1, inplace=True)

    # Cleaning rows where the values are NA or NONE for all columns except for
    # the "create_date" column as create_date is defaulted to represent today's date
    df = df[~(df.notna().sum(axis=1) <= 1)]

    # If there are no messages to upload after processing
    if df.shape[0] == 0:
        return

    # Connect to DB
    try:
        print(f"Process {os.getpid()} Establishing Connection to DB")
        db = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        conn = db.connect()
        print(f"Process {os.getpid()} Successfully Connected to DB")

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(
            f"Exception Occured \n Process {os.getpid()}, \n data: {data}, \n Error: {e}, \n ErrorTraceback: \n{error_traceback}"
        )
        raise Exception("Unable to Connect to DB")

    # Push to DB
    transaction = conn.begin()  # Start Transaction

    try:
        # Push the DataFrame to PostgreSQL
        df.to_sql(name="user_logins", con=conn, if_exists="append", index=False)

        # Commit the transaction
        transaction.commit()
        print(f"Process {os.getpid()}: user_logins has been successfully committed.")

    except Exception as e:
        # Rollback the transaction if there's an error
        transaction.rollback()

        error_traceback = traceback.format_exc()
        print(
            f"Exception Occured \n Process {os.getpid()}, \n data: {data}, \n Error: {e}, \n ErrorTraceback: \n{error_traceback}"
        )
        raise Exception(
            "Error occurred in updating user_logins. Transaction has been rolled back."
        )

    conn.close()


if __name__ == "__main__":
    # Get number of cpus available
    num_cpus = get_available_cpus()

    # Connect to Client
    try:
        sqs = boto3.client(
            "sqs",
            endpoint_url=QUEUE_END_POINT,
            region_name=AWS_REGION,
            aws_access_key_id="",
            aws_secret_access_key="",
        )

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(
            f"Exception Occured: \n Error: {e}, \n ErrorTraceback: \n{error_traceback}"
        )
        raise Exception("Unable to connect to Boto3 Client Error")

    # Get approximate number of messages available for retrieval
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=QUEUE_URL, AttributeNames=["ApproximateNumberOfMessages"]
        )

        num_messages = int(response["Attributes"]["ApproximateNumberOfMessages"])

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(
            f"Exception Occured: \n Error: {e}, \n ErrorTraceback: \n{error_traceback}"
        )
        raise Exception("Unable to Fetch Approximate Number of Messages")

    if num_messages:
        # Receive & Process messages
        print("Minimum Number of messages to be processed: ", num_messages)

        count = 0

        # Calculate Number of messages to retrieve in each batch
        batch_size = ceil(num_messages / num_cpus)

        data = []  # List to store each message body
        while True:
            # Receive messages

            try:
                response = sqs.receive_message(
                    QueueUrl=QUEUE_URL,
                    MaxNumberOfMessages=batch_size,
                    VisibilityTimeout=0,
                    WaitTimeSeconds=0,
                )

            except Exception as e:
                error_traceback = traceback.format_exc()
                print(
                    f"Exception Occured: \n Error: {e}, \n ErrorTraceback: \n{error_traceback}"
                )
                raise Exception("Unable to Fetch Messages")

            # Check if there are no more messages
            if "Messages" not in response:
                print("No more messages to receive")
                break

            # Process messages
            for message in response["Messages"]:
                # print("Message: ", message["Body"])

                try:
                    # Delete received message
                    sqs.delete_message(
                        QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"]
                    )

                except Exception as e:
                    error_traceback = traceback.format_exc()
                    print(
                        f"Exception Occured: \n Error: {e}, \n ErrorTraceback: \n{error_traceback}"
                    )
                    raise Exception("Unable to Delete Read Messages")

                data.append(message["Body"])

            count += len(data)

        # Removing Duplicate messages
        data = list(set(data))

        # List to hold process_data function call objects via multiprocessing
        processes = []

        # Re-calculate batch size after deduplicating messages
        batch_size = ceil(len(data) / num_cpus)

        # Processing the data by leveraging multi processing
        for i in range(num_cpus):
            batch = data[i * batch_size : (i + 1) * batch_size]

            if len(batch) != 0:
                try:
                    process = multiprocessing.Process(
                        target=process_data, args=(batch,)
                    )
                    processes.append(process)
                    process.start()
                except Exception as e:
                    error_traceback = traceback.format_exc()
                    print(
                        f"Exception Occured: \n Error: {e}, \n ErrorTraceback: \n{error_traceback}"
                    )
                    raise Exception("Unable to Multi-Process Messages")

        # Wait for all processes to complete
        [process.join() for process in processes]

        print("Number of Messages Processed: ", count)

    else:
        # No new messages receive
        print("No new messages to receive")

    # Close the sqs client
    sqs.close()
