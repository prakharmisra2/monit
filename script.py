import serial
import time
import csv
import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'sensor_data'),
    'user': os.getenv('DB_USER', 'sensor_user'),
    'password': os.getenv('DB_PASSWORD', 'your_secure_password'),
    'port': os.getenv('DB_PORT', '5432')
}

# Serial port configuration
ser = serial.Serial(
    port='/dev/ttyUSB0', 
    baudrate=19200,
    bytesize=serial.EIGHTBITS,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    timeout=1
)

# CSV file setup (keeping as backup)
csv_filename = f"sensor_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
csv_headers = ['Timestamp', 'Command', 'Pressure', 'Temperature', 'X', 'Y', 'Air_Value', 'Air_Status']

def init_database():
    """Initialize database connection and create table if not exists"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            command VARCHAR(10),
            pressure DECIMAL(10,4),
            temperature DECIMAL(10,4),
            x_value DECIMAL(10,6),
            y_value DECIMAL(10,6),
            air_value DECIMAL(10,4),
            air_status VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        print(conn)
        cur.execute(create_table_query)
        conn.commit()
        cur.close()
        conn.close()
        
        print("Database initialized successfully")
        return True
        
    except Exception as e:
        print(f"Database initialization error: {e}")
        return False

def parse_response(response):
    """
    Parse the response string and extract individual values
    Expected format: "A +00.963 +031.28 -0.0057 -0.0053 +000031.3    Air"
    """
    try:
        parts = response.split()
        if len(parts) >= 6:
            command = parts[0]
            pressure = float(parts[1])
            temperature = float(parts[2])
            x_value = float(parts[3])
            y_value = float(parts[4])
            air_value = float(parts[5])
            air_status = parts[6] if len(parts) > 6 else ""
            
            return {
                'command': command,
                'pressure': pressure,
                'temperature': temperature,
                'x': x_value,
                'y': y_value,
                'air_value': air_value,
                'air_status': air_status
            }
    except (ValueError, IndexError) as e:
        print(f"Error parsing response: {e}")
        return None

def log_to_database(data):
    """Log parsed data to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO sensor_readings (command, pressure, temperature, x_value, y_value, air_value, air_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        cur.execute(insert_query, (
            data['command'],
            data['pressure'],
            data['temperature'],
            data['x'],
            data['y'],
            data['air_value'],
            data['air_status']
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"Database logging error: {e}")
        return False

def log_to_csv(data, filename):
    """Log parsed data to CSV file as backup"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        with open(filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                timestamp,
                data['command'],
                data['pressure'],
                data['temperature'],
                data['x'],
                data['y'],
                data['air_value'],
                data['air_status']
            ])
    except Exception as e:
        print(f"CSV logging error: {e}")

def main():
    # Initialize database
    if not init_database():
        print("Failed to initialize database. Exiting.")
        return
    
    # Create CSV file with headers if it doesn't exist (backup)
    if not os.path.exists(csv_filename):
        with open(csv_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(csv_headers)
    
    try:
        print(f"Starting data logging to database and {csv_filename}")
        print("Press Ctrl+C to stop logging")
        
        while True:
            command = 'A\r'
            ser.write(command.encode('ascii'))
            time.sleep(0.1)
            
            response = ser.readline().decode('ascii').strip()
            print("Response:", response)
            
            # Parse the response
            parsed_data = parse_response(response)
            if parsed_data:
                # Log to database (primary)
                db_success = log_to_database(parsed_data)
                
                # Log to CSV (backup)
                log_to_csv(parsed_data, csv_filename)
                
                if db_success:
                    print(f"✓ Data logged to DB: P={parsed_data['pressure']}, T={parsed_data['temperature']}, "
                          f"X={parsed_data['x']}, Y={parsed_data['y']}, Air={parsed_data['air_value']}")
                else:
                    print(f"⚠ DB failed, CSV backup saved")
            else:
                print("Failed to parse response - skipping log entry")
            
            time.sleep(5)  # Wait before sending the next command

    except KeyboardInterrupt:
        print("\nData logging stopped by user")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        ser.close()
        print(f"Serial port closed. Data saved to database and {csv_filename}")

if __name__ == "__main__":
    main()