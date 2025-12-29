import json
import time
from kafka import KafkaProducer

# Full NSL-KDD columns
COLUMNS = ["duration", "protocol_type", "service", "flag", "src_bytes", "dst_bytes", "land", 
           "wrong_fragment", "urgent", "hot", "num_failed_logins", "logged_in", "num_compromised", 
           "root_shell", "su_attempted", "num_root", "num_file_creations", "num_shells", 
           "num_access_files", "num_outbound_cmds", "is_host_login", "is_guest_login", "count", 
           "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate", 
           "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", 
           "dst_host_srv_count", "dst_host_same_srv_rate", "dst_host_diff_srv_rate", 
           "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate", "dst_host_serror_rate", 
           "dst_host_srv_serror_rate", "dst_host_rerror_rate", "dst_host_srv_rerror_rate", "label"]

# Initialize Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(3, 3, 2) 
)

# --- TOPIC INITIALIZATION ---
# This "wakes up" the topic so Spark doesn't crash on startup
print("Initializing Kafka topic 'network_traffic'...")
producer.send('network_traffic', {"status": "init"})
producer.flush() 
print("Topic ready. Waiting 2 seconds for Kafka metadata update...")
time.sleep(2) 

print("Starting streaming data...")

try:
    with open('../data/KDDTest+.txt', 'r') as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 42:
                # Create dictionary mapping
                data = dict(zip(COLUMNS, parts[:42]))
                
                # Convert numeric strings to actual numbers for ML features
                data["duration"] = int(data["duration"])
                data["src_bytes"] = int(data["src_bytes"])
                data["dst_bytes"] = int(data["dst_bytes"])
                
                # Binary label conversion: 0 for normal, 1 for attack
                data["label"] = 0 if data["label"] == "normal" else 1

                # Send live packet data
                producer.send('network_traffic', data)
                print(f"Sent: {data['protocol_type']} traffic - Label: {data['label']}")
                
                # Speed adjusted for real-time visualization
                time.sleep(0.5) 

except FileNotFoundError:
    print("Error: KDDTest+.txt not found in ../data/ folder")
except Exception as e:
    print(f"An error occurred: {e}")