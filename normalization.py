import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import storage
from apache_beam import DoFn, ParDo, PTransform
from datetime import datetime, timedelta, timezone
# from google.cloud import bigquery
import json
import logging
import sqlite3
import itertools
import random
import string
import os
import sys

# Configure logging
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='Credentials.json'
logging.basicConfig(level=logging.INFO)

# Options Runner
options = PipelineOptions()
options.view_as(StandardOptions).runner = 'SparkRunner'

def update_last_processed_date(data_start_time, data_end_time, file_count, record_count, running_time, db_name='/home/waba/balancenotedate.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS balance_note_date (
            id INTEGER PRIMARY KEY,
            data_start_time TEXT,
            data_end_time TEXT,
            file_count INTEGER,
            record_count INTEGER,
            running_time TEXT,
            is_done TEXT
        )
    ''')
    cursor = conn.cursor()
    cursor.execute('INSERT INTO balance_note_date (data_start_time, data_end_time, file_count, record_count, running_time) VALUES (?, ?, ?, ?, ?)', 
                   (data_start_time, data_end_time, file_count, record_count, running_time))
    conn.commit()
    conn.close()
    logging.info(f"Note the processed date to SQLite: from {data_start_time} until {data_end_time}. Total records: {record_count}")

def update_to_done(db_name='/home/waba/balancenotedate.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('UPDATE balance_note_date SET is_done=true WHERE id=(SELECT MAX(ROWID) FROM balance_note_date)')
    conn.commit()
    conn.close()
    logging.info(f"Updated status to DONE .....")

def DownloadObjectFromGCP(star_time, end_time):
    logging.info("================== STARTING TO DOWNLOADING DATA ==================")
    bucket_name = 'BUCKET_NAME'
    client = storage.Client()
    prefix = "PREFIX_NAME"
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    datas = []
    for blob in blobs:
        created_time = blob.time_created
        if star_time <= created_time <= end_time:
            get_val = blob.download_as_text()
            logging.info(f"Downloaded object: {blob.name}")
            conv_json = get_val.splitlines()
            datas.append(conv_json)

    if not datas:
        logging.info("--- No Data Downloaded ---")
        return []
    else:                 
        file_count = len(datas)
        trans_value = list(itertools.chain.from_iterable(datas))
        data_count = len(trans_value)
        running_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Successfully download {data_count} lines of data from {file_count} files object.")
        update_last_processed_date(star_time, end_time, file_count, data_count, running_date)
        return trans_value

class NormalizeJson(DoFn):
    def process(self, element):
        if element == []:
            logging.info("Skip the normalization because data is empty ....")
            return []
        else:
            collection = []
            for item in element:
                list_to_item = json.loads(item)
                for subitem in list_to_item:
                    my_keys = ["_id", "accountId", "creatorId", "creatorName", "resourceName",
                               "resource", "type", "amount", "before", "after", "notes", "executeAt", "createdAt"]
                    object_data = {}
                    for key, value in subitem.items():
                        if key in my_keys:
                            if isinstance(value, dict) or isinstance(value, list):
                                if value == {}:
                                    after_norm = ""
                                else:
                                    after_norm = str(value)
                            else:
                                after_norm = value
                            object_data[key] = after_norm
                    logging.info(f"Normalize data finished...")
                    json_norm = json.loads(json.dumps(object_data, default=str))
                    collection.append(json_norm)
            yield collection

    
class WriteToGCS(DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, element):
        if element == []:
            logging.info("No Data to upload into cloud storage.....")
            return
        else:
            date_now = datetime.now()
            date_string = date_now.strftime("%Y-%m-%d")
            hour = date_now.strftime("%H")
            random_id = "".join(random.choices(string.ascii_letters + string.digits, k=6))
            filename = self.output_path + "dt=" + date_string + "/" + "hr="+ hour + "/" + "transaction-" + datetime.now().strftime("%Y-%m-%dT%H:%M:%S") + "_" + random_id
            clients = storage.Client()
            buckets = clients.bucket('waba-big-lake')
            jsonl_string = '\n'.join(json.dumps(record) for record in element)
            blob = buckets.blob(filename)
            blob.upload_from_string(jsonl_string)
            logging.info("Successfully uploaded to Big Lake")
            return 


def run_pipeline():
    pid = os.getpid()
    with open("process_balance.pid", "w") as pid_file:
        pid_file.write(str(pid))
    date_now = datetime.now(timezone.utc) - timedelta(hours=1)#x jam kebelang rentan waktu
    date_start = date_now.replace(minute=0, second=59, microsecond=0)
    date_stp = date_start + timedelta(hours=1)
    date_stop = date_stp.replace(minute=59, second=59, microsecond=999999)

    while date_start <= date_stop:
        date_start_end = date_start + timedelta(minutes=19, seconds=59, microseconds=999999)
        check_object = DownloadObjectFromGCP(date_start, date_start_end)
        if check_object == []:
            logging.info("--- FILE OBJECT IS EMPTY ---")
            date_start += timedelta(minutes=20)
            continue
        else:
            logging.info(f"================ PROCESS DATA BETWEEN {date_start} AND {date_start_end} ==================")
            with beam.Pipeline(options=options) as pipeline:
                input = (
                    pipeline
                    | "Read from pubsub" >> beam.Create(DownloadObjectFromGCP(date_start, date_start_end))
                )  

                process = (
                    input
                    | "First Batch the Process" >> beam.BatchElements(min_batch_size=1000, max_batch_size=1000)
                    | "Normall please" >> beam.ParDo(NormalizeJson())
                )

                process | "Write to GCS" >> ParDo(WriteToGCS("BUCKET_NAME"))
                update_to_done()
                date_start += timedelta(minutes=20)

    pid_file = "process_balance.pid"
    if os.path.exists(pid_file):
        os.remove(pid_file)
    return

if __name__ == "__main__":
    pid_file = "/tmp/process_balance.pid"
    if os.path.exists(pid_file):
        logging.info("======================== OTHER PROCESS IS STILL RUNNING =============================")
    else:
        start_time = datetime.now()
        logging.info(f"*************************************************************************************")
        logging.info(f"Proses dimulai pada {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"*************************************************************************************")


        run_pipeline()


        logging.info(f"*************************************************************************************")
        logging.info(f"Proses dimulai pada {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        end_time = datetime.now()
        logging.info(f"Proses selesai pada {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        duration = end_time - start_time
        logging.info(f"Total waktu yang dibutuhkan: {duration}.")

