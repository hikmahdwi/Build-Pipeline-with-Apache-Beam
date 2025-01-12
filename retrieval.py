import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window
from datetime import datetime, timedelta
import logging
import json
import os
import sys
from pymongo import MongoClient
from google.cloud import pubsub_v1
import sqlite3

# Configure logging
logging.basicConfig(level=logging.INFO)

PID_FILE = '/tmp/waba_beam_pipeline_balancelogs.pid'

def check_and_create_pid():
    if os.path.exists(PID_FILE):
        logging.error(f"Another instance is running. Exiting. {PID_FILE}")
        sys.exit(1)
    else:
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

def remove_pid():
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)

def get_last_processed_date(db_name='/home/waba/transactionmessagings.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS balancelogs_dates (
            id INTEGER PRIMARY KEY,
            last_processed_date TEXT,
            record_count INTEGER,
            start_date TEXT,
            end_date TEXT
        )
    ''')
    
    cursor.execute('SELECT last_processed_date FROM balancelogs_dates ORDER BY id DESC LIMIT 1')
    result = cursor.fetchone()
    conn.close()
    
    if result:
        logging.info(f"Last processed date retrieved: {result[0]}")
        return datetime.fromisoformat(result[0]) + timedelta(minutes=60)  #hours=1 Tambah 1 jam untuk eksekusi berikutnya
    else:
        return datetime(2024, 9, 1, 0, 0)  # Default start date jika tidak ada data

def update_last_processed_date(last_date, record_count, start_date, end_date, db_name='/home/waba/transactionmessagings.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO balancelogs_dates (last_processed_date, record_count, start_date, end_date) VALUES (?, ?, ?, ?)', 
                   (last_date, record_count, start_date, end_date))
    conn.commit()
    conn.close()
    logging.info(f"Updated last processed date to: {last_date}, Total records: {record_count}, Start Date: {start_date}, End Date: {end_date}")

def get_campaigns_from_mongodb(start_date, end_date):
    mongo_client = MongoClient('DATABASE_CONNECTION')
    db = mongo_client['DATABASE_NAME']
    collection = db['COLLECTION_NAME']
    query = {
    "$and": [
        {
            "createdAt": {
                "$gte": start_date,
                "$lte": end_date
            }
        },
    ]
}
    campaigns = list(collection.find(query))
    
    log_message = "=========================================\n"
    logging.info(f"#"*50)
    logging.info(f"{datetime.now()} - Data Get from {start_date} to {end_date}, Total Data: {len(campaigns)}\n")
    logging.info(f"Fetched {len(campaigns)} campaigns from MongoDB between {start_date} and {end_date}")
    logging.info(f"Done Save Result {len(campaigns)}  to File /home/waba/balancelogsgetdata.txt")
    logging.info(f"#"*50)
    log_message += "=========================================\n"

    return campaigns

# Function to format the campaign data
def format_campaign_data2(record):
    try:
        # Convert the MongoDB document to a JSON object
        return json.dumps(record, default=str)  # Use default=str to handle non-serializable types like ObjectId
    except Exception as e:
        logging.error(f"Error formatting record to JSON: {e}")
        return None

def validate_json(data):
    try:
        json.loads(data)
        return True
    except json.JSONDecodeError:
        return False

def format_campaign_data(record):
    try:
        json_data = json.dumps(record, default=str)
        if validate_json(json_data):
            return json.loads(json_data)  # Convert to Python dictionary
        else:
            raise ValueError("Invalid JSON format")
    except Exception as e:
        logging.error(f"Error validating record: {e}")
        return None

def batch_data(elements, batch_size=10):
    # Membagi elemen menjadi batch dengan ukuran tertentu
    for i in range(0, len(elements), batch_size):
        logging.info(f"Creating batch: {i // batch_size + 1} with {len(elements[i:i+batch_size])} records")
        yield elements[i:i+batch_size]



class PublishToPubSub(beam.DoFn):
    def __init__(self, topic):
        self.topic = topic

    def start_bundle(self):
        from google.cloud import pubsub_v1
        self.publisher = pubsub_v1.PublisherClient()

    def process(self, batch):
        try:
            # Batch adalah kumpulan data dalam format JSON
            batch_size = len(batch)  # Menghitung jumlah elemen dalam batch
            message_data = json.dumps(batch).encode('utf-8')  # Serialize seluruh batch
            
            future = self.publisher.publish(self.topic, message_data)
            message_id = future.result()

            # Logging info dengan jumlah data dalam batch
            logging.info(f"*************************************************************************************")
            logging.info(f"Batch of {batch_size} records sent successfully. Message ID: {message_id}")
            logging.info(f"*************************************************************************************")

        except Exception as e:
            logging.error(f"Failed to send batch to Pub/Sub: {e}")
            raise


class PublishToPubSubPerLine(beam.DoFn):
    def __init__(self, topic):
        self.topic = topic

    def start_bundle(self):
        from google.cloud import pubsub_v1
        self.publisher = pubsub_v1.PublisherClient()

    def process(self, element):
        # Element adalah batch data yang sudah dibagi
        try:
            message_data = json.dumps(element).encode('utf-8')
            future = self.publisher.publish(self.topic, message_data)
            message_id = future.result()
            logging.info(f"*************************************************************************************")
            logging.info(f"Batch sent successfully. Message ID: {message_id}")
            logging.info(f"*************************************************************************************")
        except Exception as e:
            logging.error(f"Failed to send message to Pub/Sub: {e}")
            raise

def get_total_data_count(start_date, end_date):
    mongo_client = MongoClient('mongodb://superead:MorX4ZfTiwKBro6@209.97.171.165:27017')
    db = mongo_client['waba-balance-logs-DB']
    collection = db['balancelogs']
    query = {
        "$and": [
            {
                "createdAt": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            },
        ]
    }
    total_count = collection.count_documents(query)
    logging.info(f"Total data count between {start_date} and {end_date}: {total_count}")
    return total_count

def get_campaigns_in_batches(start_date, end_date, interval_minutes=20):
    current_start = start_date
    while current_start < end_date:
        current_end = current_start + timedelta(minutes=interval_minutes) - timedelta(microseconds=1)
        logging.info(f"Fetching data between {current_start} and {current_end}")
        batch_data = get_campaigns_from_mongodb(current_start, current_end)
        yield batch_data
        current_start += timedelta(minutes=interval_minutes)

def run(pubsub_topic):
    check_and_create_pid()
    try:
        options = PipelineOptions([
            "--runner=SparkRunner",
            "--job_endpoint=10.15.0.22:8099",
            "--spark_version=3",
            "--environment_type=LOOPBACK"
        ])

        last_processed_date = get_last_processed_date()

        if last_processed_date >= datetime.now() - timedelta(hours=25):
            logging.info("Last processed date is within the last hour, stopping process.")
            return

        start_date = last_processed_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(hours=23, minutes=59, seconds=59)

        total_data_count = get_total_data_count(start_date, end_date)

        if total_data_count > 20000:
            logging.info("Total data exceeds 20,000. Fetching data in 20-minute intervals.")
            input_data_batches = list(get_campaigns_in_batches(start_date, end_date))
            record_count = 0

            for batch in input_data_batches:
                if batch:
                    record_count += len(batch)
                    ttlbatchrecs = len(batch)
                    logging.info(f"Kirim data ke pubsub : {ttlbatchrecs}")
                    with beam.Pipeline(options=options) as p:
                        (
                            p
                            | 'Create Input Data' >> beam.Create(batch) 
                            | 'Format Data' >> beam.Map(format_campaign_data)
                            | 'Batch Data' >> beam.transforms.util.BatchElements(min_batch_size=10, max_batch_size=50)
                            | 'Publish to PubSub' >> beam.ParDo(PublishToPubSub(pubsub_topic))
                        )
        else:
            logging.info("Total data is within the limit. Fetching data for the entire day.")
            input_data = get_campaigns_from_mongodb(start_date, end_date)
            record_count = len(input_data)

            with beam.Pipeline(options=options) as p:
                (
                    p
                    | 'Create Input Data' >> beam.Create(input_data)
                    | 'Format Data' >> beam.Map(format_campaign_data)                            
                    | 'Batch Data' >> beam.transforms.util.BatchElements(min_batch_size=10, max_batch_size=50)
                    | 'Publish to PubSub' >> beam.ParDo(PublishToPubSub(pubsub_topic))
                )

        update_last_processed_date(end_date.isoformat(), record_count, start_date.isoformat(), end_date.isoformat())
        logging.info(f"Finished processing {record_count} records.")
    finally:
        remove_pid()

if __name__ == '__main__':
    # Define Pub/Sub topic (projects/{project_id}/topics/{topic_name})
    # Waktu mulai
    start_time = datetime.now()
    logging.info(f"*************************************************************************************")
    logging.info(f"Proses dimulai pada {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"*************************************************************************************")

    pubsub_topic = 'projects/waba-analytics/topics/waba-balance-ingestion'
    run(pubsub_topic)


    logging.info(f"*************************************************************************************")
    logging.info(f"*************************************************************************************")
    logging.info(f"Proses dimulai pada {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    end_time = datetime.now()
    logging.info(f"Proses selesai pada {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    duration = end_time - start_time
    logging.info(f"Total waktu yang dibutuhkan: {duration}.")

