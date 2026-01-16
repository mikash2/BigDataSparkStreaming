import time
from kafka import KafkaProducer

file_path = 'input/Electric_Vehicle_Population_Data.csv'
topic_name = 'electric_cars'

print(f"Connecting to Kafka...")
producer = KafkaProducer(bootstrap_servers='localhost:9092')
print(f"Connected! Starting to read file: {file_path}")

try:
    with open(file_path, 'r') as file:
        header = file.readline()
        print(f"Skipping header: {header.strip()}")

        line_count = 0
        for line in file:
            line = line.strip()
            if line:
                producer.send(topic_name, value=line.encode('utf-8'))
                line_count += 1
                
                if line_count % 10000 == 0:
                    print(f"Sent {line_count} lines...")

        print(f"Finished! Total lines sent: {line_count}")
        
        producer.flush()
        producer.close()

except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found. Please check the folder name.")
except Exception as e:
    print(f"An error occurred: {e}")