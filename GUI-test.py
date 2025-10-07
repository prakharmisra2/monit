import customtkinter as ctk
from tkinter import messagebox
import serial
import serial.tools.list_ports
import threading
import time
import os
import csv
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ---------- Backend Functions ---------- #

def sanitize_identifier(name: str):
    """Ensure table name is SQL-safe."""
    return ''.join(c for c in name if c.isalnum() or c == '_').lower()

def init_database(db_config, table_name):
    """Initialize database and create dynamic table if not exists."""
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        messagebox.showerror("Database Error", f"Error initializing database:\n{e}")
        return False

def parse_response(response):
    """Parse sensor response string."""
    try:
        parts = response.split()
        if len(parts) >= 6:
            return {
                'command': parts[0],
                'pressure': float(parts[1]),
                'temperature': float(parts[2]),
                'x': float(parts[3]),
                'y': float(parts[4]),
                'air_value': float(parts[5]),
                'air_status': parts[6] if len(parts) > 6 else ""
            }
    except Exception as e:
        print("Parse error:", e)
    return None

def log_to_database(data, db_config, table_name):
    """Insert one parsed record into the given table."""
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        query = f"""
        INSERT INTO {table_name} 
        (command, pressure, temperature, x_value, y_value, air_value, air_status)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """
        cur.execute(query, (
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
        print("DB insert error:", e)
        return False

def log_to_csv(data, filename):
    """Write parsed record to CSV."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        with open(filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([timestamp, *data.values()])
    except Exception as e:
        print("CSV error:", e)

# ---------- GUI Class ---------- #

class SensorApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Sensor Data Logger")
        self.geometry("650x800")

        self.running = False
        self.serial_thread = None

        # --- Scrollable Frame --- #
        scrollable_frame = ctk.CTkScrollableFrame(self, width=630, height=780)
        scrollable_frame.pack(padx=10, pady=10, fill="both", expand=True)

        # ---------- Serial Configuration Section ---------- #
        ctk.CTkLabel(scrollable_frame, text="Serial Configuration", font=("Arial", 18, "bold")).pack(pady=10)
        self.com_port_var = ctk.StringVar()
        self.baud_var = ctk.StringVar(value="19200")
        self.bytesize_var = ctk.StringVar(value="8")
        self.parity_var = ctk.StringVar(value="N")
        self.stopbits_var = ctk.StringVar(value="1")
        self.command_var = ctk.StringVar(value="A")
        self.refresh_ports()

        serial_frame = ctk.CTkFrame(scrollable_frame)
        serial_frame.pack(pady=10, padx=20, fill="x")

        fields = [
            ("COM Port", self.com_port_var, "dropdown"),
            ("Baudrate", self.baud_var),
            ("Bytesize", self.bytesize_var),
            ("Parity", self.parity_var),
            ("Stopbits", self.stopbits_var),
            ("Command", self.command_var)
        ]

        for i, (label, var, *field_type) in enumerate(fields):
            ctk.CTkLabel(serial_frame, text=f"{label}:").grid(row=i, column=0, padx=5, pady=5)
            if field_type and field_type[0] == "dropdown":
                self.com_menu = ctk.CTkOptionMenu(serial_frame, variable=var, values=self.available_ports)
                self.com_menu.grid(row=i, column=1, padx=5, pady=5)
            else:
                ctk.CTkEntry(serial_frame, textvariable=var).grid(row=i, column=1, padx=5, pady=5)

        ctk.CTkButton(serial_frame, text="Refresh Ports", command=self.refresh_ports).grid(row=len(fields), column=0, columnspan=2, pady=10)

        # --- Reactor & Sensor Section --- #
        ctk.CTkLabel(scrollable_frame, text="Device Information", font=("Arial", 18, "bold")).pack(pady=10)
        info_frame = ctk.CTkFrame(scrollable_frame)
        info_frame.pack(pady=10, padx=20, fill="x")

        self.reactor_no = ctk.StringVar()
        self.sensor_name = ctk.StringVar()

        ctk.CTkLabel(info_frame, text="Reactor Number:").grid(row=0, column=0, padx=5, pady=5)
        ctk.CTkEntry(info_frame, textvariable=self.reactor_no).grid(row=0, column=1, padx=5, pady=5)

        ctk.CTkLabel(info_frame, text="Sensor Name:").grid(row=1, column=0, padx=5, pady=5)
        ctk.CTkEntry(info_frame, textvariable=self.sensor_name).grid(row=1, column=1, padx=5, pady=5)

        # --- Database Section --- #
        ctk.CTkLabel(scrollable_frame, text="Database Configuration", font=("Arial", 18, "bold")).pack(pady=10)
        db_frame = ctk.CTkFrame(scrollable_frame)
        db_frame.pack(pady=10, padx=20, fill="x")

        self.db_host = ctk.StringVar(value=os.getenv("DB_HOST", "localhost"))
        self.db_name = ctk.StringVar(value=os.getenv("DB_NAME", "sensor_data"))
        self.db_user = ctk.StringVar(value=os.getenv("DB_USER", "sensor_user"))
        self.db_pass = ctk.StringVar(value=os.getenv("DB_PASSWORD", "your_secure_password"))
        self.db_port = ctk.StringVar(value=os.getenv("DB_PORT", "5432"))

        labels = ["Host", "Database", "User", "Password", "Port"]
        vars = [self.db_host, self.db_name, self.db_user, self.db_pass, self.db_port]

        for i, (label, var) in enumerate(zip(labels, vars)):
            ctk.CTkLabel(db_frame, text=f"{label}:").grid(row=i, column=0, padx=5, pady=5)
            ctk.CTkEntry(db_frame, textvariable=var, show="*" if label == "Password" else "").grid(row=i, column=1, padx=5, pady=5)

        # --- Control Buttons --- #
        ctk.CTkButton(scrollable_frame, text="Start Logging", command=self.start_logging, fg_color="green").pack(pady=10)
        ctk.CTkButton(scrollable_frame, text="Stop Logging", command=self.stop_logging, fg_color="red").pack(pady=5)

        self.status_label = ctk.CTkLabel(scrollable_frame, text="Status: Idle", text_color="gray")
        self.status_label.pack(pady=10)

    def refresh_ports(self):
        ports = serial.tools.list_ports.comports()
        self.available_ports = [p.device for p in ports] or ["No Ports Found"]
        if hasattr(self, 'com_menu'):
            self.com_menu.configure(values=self.available_ports)
        self.com_port_var.set(self.available_ports[0])

    def start_logging(self):
        if self.running:
            return

        if not self.reactor_no.get() or not self.sensor_name.get():
            messagebox.showerror("Input Error", "Please enter both Reactor Number and Sensor Name.")
            return

        # Dynamic table and CSV name
        safe_sensor = sanitize_identifier(self.sensor_name.get())
        table_name = f"r{self.reactor_no.get()}_{safe_sensor}_sensor_data"
        csv_filename = f"{table_name}.csv"  # fixed file name per device

        db_config = {
            'host': self.db_host.get(),
            'database': self.db_name.get(),
            'user': self.db_user.get(),
            'password': self.db_pass.get(),
            'port': self.db_port.get()
        }

        if not init_database(db_config, table_name):
            return

        # Create CSV headers only if file does not exist
        if not os.path.exists(csv_filename):
            with open(csv_filename, 'w', newline='') as f:
                csv.writer(f).writerow(['Timestamp', 'Command', 'Pressure', 'Temperature', 'X', 'Y', 'Air_Value', 'Air_Status'])

        self.running = True
        self.status_label.configure(text=f"Logging to {csv_filename}", text_color="green")

        # Start serial thread
        self.serial_thread = threading.Thread(
            target=self.serial_loop,
            args=(db_config, table_name, csv_filename),
            daemon=True
        )
        self.serial_thread.start()

    def stop_logging(self):
        self.running = False
        self.status_label.configure(text="Status: Stopped", text_color="red")

    def serial_loop(self, db_config, table_name, csv_filename):
        try:
            ser = serial.Serial(
                port=self.com_port_var.get(),
                baudrate=int(self.baud_var.get()),
                bytesize=int(self.bytesize_var.get()),
                parity=self.parity_var.get(),
                stopbits=float(self.stopbits_var.get()),
                timeout=1
            )

            command = self.command_var.get() + "\r"
            while self.running:
                ser.write(command.encode('ascii'))
                time.sleep(0.1)
                response = ser.readline().decode('ascii').strip()
                print("Response:", response)
                data = parse_response(response)
                if data:
                    log_to_database(data, db_config, table_name)
                    log_to_csv(data, csv_filename)
                time.sleep(5)
            ser.close()
        except Exception as e:
            messagebox.showerror("Serial Error", f"Error in serial communication:\n{e}")
            self.stop_logging()

# ---------- Run App ---------- #
if __name__ == "__main__":
    ctk.set_appearance_mode("System")
    ctk.set_default_color_theme("blue")
    app = SensorApp()
    app.mainloop()
