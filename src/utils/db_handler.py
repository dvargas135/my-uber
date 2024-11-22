import mysql.connector as msc
from src.utils.rich_utils import RichConsoleUtils
from threading import local

class DatabaseHandler:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.local_storage = local()  # Thread-local storage for connection and cursor

    def connect(self):
        if not hasattr(self.local_storage, 'connection') or not self.local_storage.connection.is_connected():
            self.local_storage.connection = msc.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.local_storage.cursor = self.local_storage.connection.cursor()

    def close(self):
        if hasattr(self.local_storage, 'cursor'):
            self.local_storage.cursor.close()
            del self.local_storage.cursor
        if hasattr(self.local_storage, 'connection'):
            self.local_storage.connection.close()
            del self.local_storage.connection

    def get_cursor(self):
        self.connect()
        return self.local_storage.cursor

    def get_connection(self):
        self.connect()
        return self.local_storage.connection
    
    def show_tables(self):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        cursor.execute("SHOW TABLES")
        records = cursor.fetchall()
        print(records)
        self.close()

    def add_taxi(self, taxi_id, pos_x, pos_y, speed, status):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        query = """
        INSERT INTO taxis (taxi_id, pos_x, pos_y, speed, status) 
        VALUES (%s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE pos_x = VALUES(pos_x), pos_y = VALUES(pos_y), speed = VALUES(speed), status = VALUES(status)
        """
        values = (taxi_id, pos_x, pos_y, speed, status)
        cursor.execute(query, values)
        connection.commit()
        self.close()
    
    def update_taxi_position(self, taxi_id, pos_x, pos_y):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        query = "UPDATE taxis SET pos_x = %s, pos_y = %s WHERE taxi_id = %s"
        values = (pos_x, pos_y, taxi_id)
        cursor.execute(query, values)
        connection.commit()
        self.close()
    
    def set_taxi_status(self, taxi_id, status):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        query = "UPDATE taxis SET status = %s WHERE taxi_id = %s"
        values = (status, taxi_id)
        cursor.execute(query, values)
        connection.commit()
        self.close()
    
    def mark_taxi_available(self, taxi_id):
        self.set_taxi_status(taxi_id, "available")
    
    def add_user_request(self, user_id, pos_x, pos_y, waiting_time=30):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        query = """
        INSERT INTO users (user_id, pos_x, pos_y, waiting_time) 
        VALUES (%s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE pos_x = VALUES(pos_x), pos_y = VALUES(pos_y), waiting_time = VALUES(waiting_time)
        """
        values = (user_id, pos_x, pos_y, waiting_time)
        cursor.execute(query, values)
        connection.commit()
        self.close()
    
    def assign_taxi_to_user(self, user_id, taxi_id):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        # Create the assignment
        query_assignment = """
        INSERT INTO assignments (user_id, taxi_id, status) 
        VALUES (%s, %s, %s) 
        ON DUPLICATE KEY UPDATE taxi_id = VALUES(taxi_id), status = VALUES(status)
        """
        values_assignment = (user_id, taxi_id, "assigned")
        cursor.execute(query_assignment, values_assignment)

        # Update the taxi status
        query_taxi = "UPDATE taxis SET status = %s, connected = %s WHERE taxi_id = %s"
        values_taxi = ("unavailable", False, taxi_id)
        cursor.execute(query_taxi, values_taxi)

        connection.commit()
        self.close()

    def record_heartbeat(self, taxi_id):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        # try:
        query = """
        INSERT INTO heartbeat (taxi_id) 
        VALUES (%s) 
        ON DUPLICATE KEY UPDATE timestamp = CURRENT_TIMESTAMP
        """
        values = (taxi_id,)
        cursor.execute(query, values)
        connection.commit()
        # finally:
        self.close()

    def get_available_taxis(self):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        query = "SELECT * FROM taxis WHERE status = %s AND connected = %s"
        values = ("available", True)
        cursor.execute(query, values)
        taxis = cursor.fetchall()
        self.close()
        return taxis
    
    def update_taxi_connected_status(self, taxi_id, connected):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        # try:
        query = "UPDATE taxis SET connected = %s WHERE taxi_id = %s"
        values = (connected, taxi_id)
        cursor.execute(query, values)
        connection.commit()
        # except Exception as e:
            # self.console_utils.print(f"Error updating taxi connected status: {e}", 3)
        # finally:
        self.close()

    def taxi_exists(self, taxi_id):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        # try:
        query = "SELECT COUNT(*) FROM taxis WHERE taxi_id = %s"
        cursor.execute(query, (taxi_id,))
        result = cursor.fetchone()
        return result[0] > 0
        # except Exception as e:
            # self.console_utils.print(f"Error checking if taxi exists: {e}", 3)
            # return False
        # finally:
        self.close()
    
    def get_all_taxis(self):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        query = "SELECT taxi_id, pos_x, pos_y, speed, status, connected FROM taxis"
        cursor.execute(query)
        taxis = cursor.fetchall()
        self.close()
        return taxis

    def get_taxi_by_id(self, taxi_id):
        # self.connect()
        cursor = self.get_cursor()
        connection = self.get_connection()
        query = "SELECT * FROM taxis WHERE taxi_id = %s"
        cursor.execute(query, (taxi_id,))
        taxi = cursor.fetchone()
        self.close()
        return taxi

# Example usage:
# db_handler = DatabaseHandler(host="192.168.1.13", user="root", password="123456789", database="taxi_dispatch")
# db_handler.show_tables()

# To insert data into a table, for example:
# db_handler.insert_data("table_name", ["column1", "column2"], ["value1", "value2"])
