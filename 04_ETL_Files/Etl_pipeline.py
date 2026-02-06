

import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import logging
import os
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_process.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class LibraryETL:
    def __init__(self, db_config=None):
        self.db_config = db_config or {}
        self.connection = None
        self.cursor = None
        self.valid_date_keys = set()
        self.valid_student_keys = set()
        self.valid_resource_keys = set()
        self.valid_room_keys = set()                  
        self.student_id_to_key = {}       
        self.resource_id_to_key = {}      
        self.default_department_key = None 

    # - connect
    def connect_database(self):
        self.connection = mysql.connector.connect(
            host=self.db_config.get('host','localhost'),
            user=self.db_config.get('user','root'),
            password=self.db_config.get('password',''),
            database=self.db_config.get('database','university library analytics'),
            auth_plugin="mysql_native_password"
        )
        self.cursor = self.connection.cursor(dictionary=True)
        logging.info("✓ Database connected")

    def close_database(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logging.info("Database closed")

    # - helpers
    def get_date_key(self, value):
        if not value or str(value).upper() in ['NULL','UNKNOWN','NAN','']:
            return 20240101
        
        value_str = str(value).strip()
        for fmt in ('%Y-%m-%d','%d/%m/%Y','%m/%d/%Y','%Y/%m/%d','%d-%m-%Y'):
            try:
                dt = datetime.strptime(value_str, fmt)
                dk = int(dt.strftime('%Y%m%d'))
                return max(20100101, min(dk, 20351231))
            except:
                continue
        return 20240101

    def safe_int(self, value):
        try:
            return int(float(value))
        except:
            return 0

    def safe_float(self, value):
        try:
            return float(value)
        except:
            return 0.0

    def standardize_room(self, value):
        """Extract digits from any room string and return R + digits.
        Examples: 'Room101' -> 'R101', '101' -> 'R101', 'R-101' -> 'R101'
        """
        if not value or str(value).upper() in ['NULL', 'UNKNOWN', '']:
            return 'R-UNKNOWN'
        digits = re.sub(r'\D', '', str(value))          
        return f"R{digits}" if digits else 'R-UNKNOWN'

    #  dim_date
    def fix_dim_date_table(self):
        """Fix broken dim_date table"""
        logging.info("Checking dim_date table...")
        
        self.cursor.execute("SELECT MIN(date_key) as min_k, MAX(date_key) as max_k, COUNT(*) as cnt FROM dim_date")
        result = self.cursor.fetchone()
        
        if result['cnt'] > 0 and result['max_k'] < 1000000:
            logging.warning(f" dim_date is BROKEN! Has date_keys {result['min_k']}-{result['max_k']}")
            logging.warning("Deleting and rebuilding dim_date...")
            self.cursor.execute("DELETE FROM dim_date")
            self.connection.commit()
        
        self.cursor.execute("SELECT COUNT(*) as c FROM dim_date WHERE date_key >= 20100101 AND date_key <= 20351231")
        if self.cursor.fetchone()['c'] > 9000:
            logging.info("✓ dim_date already correctly populated")
            self.load_valid_date_keys()
            return

        logging.info("Rebuilding dim_date (2010-2035)...")
        start   = datetime(2010,1,1)
        end     = datetime(2035,12,31)
        current = start
        rows    = []

        while current <= end:
            rows.append((
                int(current.strftime('%Y%m%d')),
                current.strftime('%Y-%m-%d'),
                current.strftime('%A'),
                current.day,
                current.timetuple().tm_yday,
                current.isocalendar()[1],
                current.month,
                current.strftime('%B'),
                (current.month-1)//3 + 1,
                current.year,
                1 if current.weekday()>=5 else 0,
                0
            ))
            current += timedelta(days=1)

        self.cursor.executemany("""
            INSERT INTO dim_date
            (date_key, full_date, day_of_week, day_of_month, day_of_year, week_of_year,
             month, month_name, quarter, year, is_weekend, is_holiday)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)
        self.connection.commit()
        logging.info(f"dim_date rebuilt with {len(rows)} records")
        self.load_valid_date_keys()

    def load_valid_date_keys(self):
        self.cursor.execute("SELECT date_key FROM dim_date")
        self.valid_date_keys = {row['date_key'] for row in self.cursor.fetchall()}
        if self.valid_date_keys:
            logging.info(f" Loaded {len(self.valid_date_keys)} valid date_keys")

    # CSV parsing
    def parse_digital_usage_csv(self, path):
        """Parse the malformed digital_usage.csv file"""
        logging.info(f"Parsing {path}...")
        
        with open(path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if not lines:
            return pd.DataFrame()
        
        header = lines[0].strip()
        header = re.sub(r'"+"', '', header)
        header = header.replace('""', '')
        
        columns = [col.strip().strip('"') for col in header.split(';')]
        
        # Fix duplicate column names
        seen = {}
        unique_columns = []
        for col in columns:
            if col in seen:
                seen[col] += 1
                unique_columns.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                unique_columns.append(col)
        
        data_rows = []
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            line = re.sub(r'"+"', '"', line)
            values = [val.strip().strip('"') for val in line.split(';')]
            data_rows.append(values)
        
        df = pd.DataFrame(data_rows, columns=unique_columns)
        logging.info(f" Parsed {len(df)} digital usage records")
        
        return df

    #  staging
    def load_staging(self, digital_path, bookings_path):
        # Books from database
        self.cursor.execute("SELECT * FROM book_transactions")
        df_books = pd.DataFrame(self.cursor.fetchall()).fillna('NULL')
        logging.info(f"Loaded {len(df_books)} book transactions")

        # Digital usage
        df_digital = self.parse_digital_usage_csv(digital_path)
        
        if df_digital.empty:
            df_digital = pd.DataFrame({
                'Date': ['2024-01-01'],
                'ResourceType': ['E-Book'],
                'DownloadCount': [0],
                'Duration_Minutes': [0]
            })
        
        df_digital.columns = [col.strip() for col in df_digital.columns]
        
        col_map = {}
        for col in df_digital.columns:
            col_lower = col.lower()
            if 'date' in col_lower and 'Date' not in col_map.values():
                col_map[col] = 'Date'
            elif ('resourcetype_1' in col_lower or (col == 'ResourceType_1')):
                col_map[col] = 'ResourceType'
            elif 'resource' in col_lower and 'ResourceType' not in col_map.values():
                col_map[col] = 'ResourceType'
            elif 'download' in col_lower or 'count' in col_lower:
                col_map[col] = 'DownloadCount'
            elif 'duration' in col_lower or 'minute' in col_lower:
                col_map[col] = 'Duration_Minutes'
        
        df_digital = df_digital.rename(columns=col_map)
        
        if 'Date'            not in df_digital.columns: df_digital['Date']            = '2024-01-01'
        if 'ResourceType'    not in df_digital.columns: df_digital['ResourceType']    = 'E-Book'
        if 'DownloadCount'   not in df_digital.columns: df_digital['DownloadCount']   = 1
        if 'Duration_Minutes' not in df_digital.columns: df_digital['Duration_Minutes'] = 30
        
        df_digital['DownloadCount']   = df_digital['DownloadCount'].apply(self.safe_int)
        df_digital['Duration_Minutes'] = df_digital['Duration_Minutes'].apply(self.safe_int)

        # Room bookings
        df_rooms = pd.read_csv(bookings_path).fillna('NULL')
        logging.info(f"Loaded {len(df_rooms)} room bookings")
        
        if 'DurationHours' not in df_rooms.columns:
            df_rooms['DurationHours'] = df_rooms.get('Duration', 1.0)
        
        df_rooms['DurationHours'] = df_rooms['DurationHours'].apply(self.safe_float)
        
        return df_books, df_digital, df_rooms

    #  dimensions
    def populate_dimensions(self, df_books, df_digital, df_rooms):

        # ---------- dim_department ----------
        
        self.cursor.execute("""
            INSERT IGNORE INTO dim_department (department_name)
            VALUES ('Unknown')
        """)
        self.connection.commit()

        # Fetch the auto-incremented department_id for 'Unknown'
        self.cursor.execute("SELECT department_id FROM dim_department WHERE department_name = 'Unknown'")
        dept_row = self.cursor.fetchone()
        if dept_row:
            self.default_department_key = dept_row['department_id']   # maps -> fact.department_key
            logging.info(f" Default department_key = {self.default_department_key} (from dim_department.department_id)")
        else:
            logging.error(" Could not find 'Unknown' row in dim_department after INSERT IGNORE.")
            raise Exception("No valid department_id found in dim_department")

        # ---------- dim_student ----------
        self.cursor.execute(
            "INSERT IGNORE INTO dim_student (student_id, student_type, enrollment_date, is_active) "
            "VALUES ('UNKNOWN', 'Unknown', '2020-01-01', 1)"
        )

        students = set()
        if 'StudentID' in df_books.columns:
            students |= set(df_books['StudentID'])
        if 'StudentID' in df_rooms.columns:
            students |= set(df_rooms['StudentID'])
        
        invalid = ['NULL','UNKNOWN','STAFF','FACULTY','DIGITAL','NAN']
        students = {s for s in students if s and not any(i in str(s).upper() for i in invalid)}

        for s in sorted(students):
            self.cursor.execute(
                "INSERT IGNORE INTO dim_student (student_id, student_type, enrollment_date, is_active) "
                "VALUES (%s, 'Student', '2024-01-01', 1)",
                (s,)
            )
        
        self.connection.commit()
        logging.info(f" Inserted {len(students)} students")

        # Load student_id -> student_key mapping
        self.cursor.execute("SELECT student_key, student_id FROM dim_student")
        for row in self.cursor.fetchall():
            self.student_id_to_key[row['student_id']] = row['student_key']
        logging.info(f" Loaded {len(self.student_id_to_key)} student mappings")

        # ---------- dim_room ----------
        
        self.cursor.execute("""
            DELETE FROM dim_room
            WHERE room_key != 'R-UNKNOWN'
              AND room_key NOT REGEXP '^R[0-9]+$'
        """)
        self.connection.commit()
        logging.info("  Cleaned junk rows from dim_room")

        # Insert the fallback row first
        self.cursor.execute(
            "INSERT IGNORE INTO dim_room (room_key, room_number, room_description, capacity, is_active) "
            "VALUES ('R-UNKNOWN', 'UNKNOWN', 'Unknown', NULL, 1)"
        )

        # Insert one canonical row per unique room in the source CSV
        if 'RoomNumber' in df_rooms.columns:
            for r in sorted(set(df_rooms['RoomNumber'])):
                rk = self.standardize_room(r)          
                if rk == 'R-UNKNOWN':
                    continue                           
                self.cursor.execute(
                    "INSERT IGNORE INTO dim_room (room_key, room_number, room_description, capacity, is_active) "
                    "VALUES (%s, %s, 'Study Room', NULL, 1)",
                    (rk, rk)
                )

        self.connection.commit()

        # Cache valid room_keys so we can validate before fact insert
        self.cursor.execute("SELECT room_key FROM dim_room")
        self.valid_room_keys = {row['room_key'] for row in self.cursor.fetchall()}
        logging.info(f" dim_room rebuilt – valid keys: {sorted(self.valid_room_keys)}")

        # ---------- dim_resource ----------
        resource_inserts = [
            ('RES-BOOK',    'Physical Book', 'Book',    'Physical', 'Various', 'Various', None),
            ('RES-E-BOOK',  'E-Book',        'E-Book',  'Digital',  'Various', 'Various', None),
            ('RES-JOURNAL', 'Journal',       'Journal', 'Digital',  'Various', 'Various', None),
            ('RES-ARTICLE', 'Article',       'Article', 'Digital',  'Various', 'Various', None)
        ]
        
        for res_id, res_name, res_type, res_cat, author, pub, year in resource_inserts:
            self.cursor.execute(
                "INSERT IGNORE INTO dim_resource "
                "(resource_id, resource_name, resource_type, resource_category, author, publisher, publication_year) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (res_id, res_name, res_type, res_cat, author, pub, year)
            )

        self.connection.commit()
        
        # Load resource_id -> resource_key mapping
        self.cursor.execute("SELECT resource_key, resource_id FROM dim_resource")
        for row in self.cursor.fetchall():
            self.resource_id_to_key[row['resource_id']] = row['resource_key']
        
        logging.info(f"✓ Loaded {len(self.resource_id_to_key)} resource mappings")
        logging.info(f"  Resource IDs: {list(self.resource_id_to_key.keys())}")
        
        # Cache valid key sets for validation
        self.cursor.execute("SELECT student_key FROM dim_student")
        self.valid_student_keys = {row['student_key'] for row in self.cursor.fetchall()}
        
        self.cursor.execute("SELECT resource_key FROM dim_resource")
        self.valid_resource_keys = {row['resource_key'] for row in self.cursor.fetchall()}
        
        logging.info(f"✓ Dimensions populated")
        logging.info(f"  Valid students:    {len(self.valid_student_keys)}")
        logging.info(f"  Valid resources:   {len(self.valid_resource_keys)}")
        logging.info(f"  department_key:    {self.default_department_key}")

    #  fact table
    def populate_fact_usage(self, df_books, df_digital, df_rooms):

        # Safety gate – we cannot proceed without a department_key
        if self.default_department_key is None:
            logging.error(" default_department_key is None – aborting fact insert")
            return

        records = []
        skipped = {'no_date': 0, 'no_student': 0, 'no_resource': 0, 'no_room': 0}

        # ---- Books ----
        logging.info(f"Processing {len(df_books)} book transactions...")
        for _, r in df_books.iterrows():
            date_key    = self.get_date_key(r.get('CheckoutDate'))
            student_id  = r.get('StudentID')
            student_key = self.student_id_to_key.get(student_id,
                              self.student_id_to_key.get('UNKNOWN'))
            resource_key = self.resource_id_to_key.get('RES-BOOK')
            
            if date_key not in self.valid_date_keys:
                skipped['no_date'] += 1; continue
            if student_key is None or student_key not in self.valid_student_keys:
                skipped['no_student'] += 1; continue
            if resource_key is None or resource_key not in self.valid_resource_keys:
                skipped['no_resource'] += 1; continue

            # Tuple order matches the INSERT column list below:
            
            records.append((
                date_key,
                student_key,
                self.default_department_key,   
                resource_key,                  
                None,                          
                None,                          
                0,                             
                1,                             
                'Book Transaction'             
            ))

        logging.info(f"  ✓ Added {sum(1 for r in records if r[8] == 'Book Transaction')} book records")

        # ---- Digital ----
        logging.info(f"Processing {len(df_digital)} digital usage records...")
        
        resource_type_map = {
            'E-Book':  'RES-E-BOOK',
            'E-book':  'RES-E-BOOK',
            'e-Book':  'RES-E-BOOK',
            'ebook':   'RES-E-BOOK',
            'Journal': 'RES-JOURNAL',
            'journal': 'RES-JOURNAL',
            'Article': 'RES-ARTICLE',
            'article': 'RES-ARTICLE',
        }
        
        for idx, r in df_digital.iterrows():
            date_key = self.get_date_key(r.get('Date'))
            
            res_type_raw = r.get('ResourceType', 'E-Book')
            if isinstance(res_type_raw, pd.Series):
                resource_type = str(res_type_raw.iloc[-1] if len(res_type_raw) > 0 else 'E-Book').strip()
            else:
                resource_type = str(res_type_raw).strip()
            
            resource_id  = resource_type_map.get(resource_type, 'RES-E-BOOK')
            resource_key = self.resource_id_to_key.get(resource_id)
            student_key  = self.student_id_to_key.get('UNKNOWN')
            
            if date_key not in self.valid_date_keys:
                skipped['no_date'] += 1; continue
            if student_key is None:
                skipped['no_student'] += 1; continue
            if resource_key is None:
                skipped['no_resource'] += 1; continue

            records.append((
                date_key,
                student_key,
                self.default_department_key,
                resource_key,
                None,                                          
                None,                                          
                self.safe_int(r.get('Duration_Minutes', 0)),
                self.safe_int(r.get('DownloadCount', 0)),
                'Digital Usage'
            ))

        logging.info(f"   Added {sum(1 for r in records if r[8] == 'Digital Usage')} digital records")

        # ---- Rooms ----
        logging.info(f"Processing {len(df_rooms)} room bookings...")
        for _, r in df_rooms.iterrows():
            date_key    = self.get_date_key(r.get('BookingDate'))
            student_id  = r.get('StudentID')
            student_key = self.student_id_to_key.get(student_id,
                              self.student_id_to_key.get('UNKNOWN'))
            room_key    = self.standardize_room(r.get('RoomNumber', 'R-UNKNOWN'))

            # Validate room_key exists in dim_room; fall back to R-UNKNOWN if not
            if room_key not in self.valid_room_keys:
                logging.debug(f"  room_key '{room_key}' not in dim_room, falling back to R-UNKNOWN")
                room_key = 'R-UNKNOWN'
            
            if date_key not in self.valid_date_keys:
                skipped['no_date'] += 1; continue
            if student_key is None:
                skipped['no_student'] += 1; continue

            records.append((
                date_key,
                student_key,
                self.default_department_key,
                None,                                                     
                room_key,                                                  
                None,                                                      
                int(self.safe_float(r.get('DurationHours', 1.0)) * 60),
                0,
                str(r.get('Purpose', 'Study'))
            ))

        logging.info(f"   Added {sum(1 for r in records if r[8] not in ['Book Transaction','Digital Usage'])} room records")
        logging.info(f"\n Total records prepared: {len(records)}")
        logging.info(f"  Skipped – no_date: {skipped['no_date']}, no_student: {skipped['no_student']}, "
                     f"no_resource: {skipped['no_resource']}, no_room: {skipped['no_room']}")
        
        if len(records) == 0:
            logging.error(" No valid records to insert!")
            return
        
        # Column order MUST match the tuple order built above
        self.cursor.executemany("""
            INSERT INTO fact_library_usage
            (date_key, student_key, department_key, resource_key, room_key,
             time_slot_key, duration_minutes, quantity, purpose)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, records)
        self.connection.commit()
        logging.info(f"\n fact_library_usage populated with {len(records)} records!")

    #  orchestrator
    def run_etl(self, digital_path, bookings_path):
        try:
            self.connect_database()
            self.fix_dim_date_table()
            df_books, df_digital, df_rooms = self.load_staging(digital_path, bookings_path)
            self.populate_dimensions(df_books, df_digital, df_rooms)
            self.populate_fact_usage(df_books, df_digital, df_rooms)
            self.close_database()
            
            
            logging.info(" ETL COMPLETED SUCCESSFULLY ")
            
        except Exception as e:
            logging.error(f" ETL FAILED: {e}")
            import traceback
            traceback.print_exc()
            if self.connection:
                self.close_database()
            raise

def main():
    base = os.path.dirname(os.path.abspath(__file__))
    etl  = LibraryETL()
    etl.run_etl(
        os.path.join(base, "digital_usage.csv"),
        os.path.join(base, "room_bookings.csv")
    )

if __name__ == "__main__":
    main()