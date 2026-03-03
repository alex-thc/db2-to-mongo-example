import sys
import time
import argparse
import random
import threading
import concurrent.futures
from datetime import datetime, timedelta
from faker import Faker
import ibm_db

# Configuration for DB2
DB2_HOST = "myip"
DB2_PORT = 50000
DB2_DATABASE = "mydb"
DB2_USER = "DB2INST1"
DB2_PASSWORD = "mypassword"

class DB2Loader:
    def __init__(self, parallel_level, duration, ratios):
        self.parallel_level = parallel_level
        self.duration = duration
        self.ratios = ratios # {"insert": X, "update": Y, "delete": Z}
        self.fake = Faker()
        self.lock = threading.Lock()
        
        # Stats
        self.stats = {
            "AGENTS": {"ins": 0, "upd": 0, "del": 0},
            "ACCOUNTS": {"ins": 0, "upd": 0, "del": 0},
            "CUSTOMERS": {"ins": 0, "upd": 0, "del": 0},
            "POLICIES": {"ins": 0, "upd": 0, "del": 0},
            "CLAIMS": {"ins": 0, "upd": 0, "del": 0},
            "TRANSACTIONS": {"ins": 0, "upd": 0, "del": 0},
        }
        self.total_ops = 0
        self.start_time = None
        
        # In-memory IDs for referential integrity
        self.ids = {
            "AGENTS": [],
            "ACCOUNTS": [],
            "CUSTOMERS": [],
            "POLICIES": [],
            "CLAIMS": [],
            "TRANSACTIONS": []
        }

    def get_connection(self):
        dsn = (
            f"DRIVER={{IBM DB2 ODBC DRIVER}};"
            f"DATABASE={DB2_DATABASE};"
            f"HOSTNAME={DB2_HOST};"
            f"PORT={DB2_PORT};"
            f"PROTOCOL=TCPIP;"
            f"UID={DB2_USER};"
            f"PWD={DB2_PASSWORD};"
        )
        return ibm_db.connect(dsn, "", "")

    def init_db(self):
        print("Initializing database with schema.sql...")
        conn = self.get_connection()
        with open("schema.sql", "r") as f:
            sql = f.read()
            # Split by semicolon and filter empty strings
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            for stmt in statements:
                try:
                    ibm_db.exec_immediate(conn, stmt)
                except Exception as e:
                    print(f"Error executing statement: {stmt[:50]}... \n{e}")
        ibm_db.close(conn)
        print("Database initialized.")

    def log_stat(self, entity, op_type):
        with self.lock:
            self.stats[entity][op_type] += 1
            self.total_ops += 1

    def generate_agent(self, conn, op):
        if op == "ins":
            agent_id = random.randint(1, 10**9)
            sql = "INSERT INTO AGENTS (AGENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, REGION, STATED_COMMISSION) VALUES (?, ?, ?, ?, ?, ?, ?)"
            stmt = ibm_db.prepare(conn, sql)
            params = (agent_id, self.fake.first_name(), self.fake.last_name(), self.fake.email(), self.fake.phone_number()[:20], self.fake.city(), round(random.uniform(1.0, 15.0), 2))
            ibm_db.execute(stmt, params)
            with self.lock: self.ids["AGENTS"].append(agent_id)
            self.log_stat("AGENTS", "ins")
        elif op == "upd" and self.ids["AGENTS"]:
            agent_id = random.choice(self.ids["AGENTS"])
            sql = "UPDATE AGENTS SET STATED_COMMISSION = ? WHERE AGENT_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (round(random.uniform(1.0, 15.0), 2), agent_id))
            self.log_stat("AGENTS", "upd")
        elif op == "del" and self.ids["AGENTS"]:
            agent_id = random.choice(self.ids["AGENTS"])
            sql = "DELETE FROM AGENTS WHERE AGENT_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (agent_id,))
            with self.lock: self.ids["AGENTS"].remove(agent_id)
            self.log_stat("AGENTS", "del")

    def generate_account(self, conn, op):
        if not self.ids["AGENTS"] and op != "del": return
        if op == "ins":
            acc_id = random.randint(1, 10**9)
            agent_id = random.choice(self.ids["AGENTS"])
            sql = "INSERT INTO ACCOUNTS (ACCOUNT_ID, ACCOUNT_NAME, AGENT_ID, STATUS) VALUES (?, ?, ?, ?)"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (acc_id, self.fake.company(), agent_id, random.choice(["Active", "Inactive", "Pending"])))
            with self.lock: self.ids["ACCOUNTS"].append(acc_id)
            self.log_stat("ACCOUNTS", "ins")
        elif op == "upd" and self.ids["ACCOUNTS"]:
            acc_id = random.choice(self.ids["ACCOUNTS"])
            sql = "UPDATE ACCOUNTS SET STATUS = ? WHERE ACCOUNT_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (random.choice(["Active", "Inactive", "Pending"]), acc_id))
            self.log_stat("ACCOUNTS", "upd")
        elif op == "del" and self.ids["ACCOUNTS"]:
            acc_id = random.choice(self.ids["ACCOUNTS"])
            sql = "DELETE FROM ACCOUNTS WHERE ACCOUNT_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (acc_id,))
            with self.lock: self.ids["ACCOUNTS"].remove(acc_id)
            self.log_stat("ACCOUNTS", "del")

    def generate_customer(self, conn, op):
        if not self.ids["ACCOUNTS"] and op != "del": return
        if op == "ins":
            cust_id = random.randint(1, 10**9)
            acc_id = random.choice(self.ids["ACCOUNTS"])
            sql = "INSERT INTO CUSTOMERS (CUSTOMER_ID, ACCOUNT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, ADDRESS, CITY, STATE, ZIP_CODE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            stmt = ibm_db.prepare(conn, sql)
            params = (cust_id, acc_id, self.fake.first_name(), self.fake.last_name(), self.fake.email(), self.fake.phone_number()[:20], self.fake.street_address(), self.fake.city(), self.fake.state(), self.fake.zipcode()[:10])
            ibm_db.execute(stmt, params)
            with self.lock: self.ids["CUSTOMERS"].append(cust_id)
            self.log_stat("CUSTOMERS", "ins")
        elif op == "upd" and self.ids["CUSTOMERS"]:
            cust_id = random.choice(self.ids["CUSTOMERS"])
            sql = "UPDATE CUSTOMERS SET PHONE = ? WHERE CUSTOMER_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (self.fake.phone_number()[:20], cust_id))
            self.log_stat("CUSTOMERS", "upd")
        elif op == "del" and self.ids["CUSTOMERS"]:
            cust_id = random.choice(self.ids["CUSTOMERS"])
            sql = "DELETE FROM CUSTOMERS WHERE CUSTOMER_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (cust_id,))
            with self.lock: self.ids["CUSTOMERS"].remove(cust_id)
            self.log_stat("CUSTOMERS", "del")

    def generate_policy(self, conn, op):
        if not self.ids["CUSTOMERS"] and op != "del": return
        if op == "ins":
            pol_id = random.randint(1, 10**9)
            cust_id = random.choice(self.ids["CUSTOMERS"])
            agent_id = random.choice(self.ids["AGENTS"]) if self.ids["AGENTS"] else None
            sql = "INSERT INTO POLICIES (POLICY_ID, CUSTOMER_ID, AGENT_ID, POLICY_TYPE, START_DATE, END_DATE, PREMIUM_AMOUNT, COVERAGE_AMOUNT, STATUS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            stmt = ibm_db.prepare(conn, sql)
            start = self.fake.date_between(start_date='-5y', end_date='today')
            end = start + timedelta(days=365)
            params = (pol_id, cust_id, agent_id, random.choice(["Life", "Auto", "Home", "Health"]), start, end, round(random.uniform(500, 5000), 2), round(random.uniform(50000, 1000000), 2), "Active")
            ibm_db.execute(stmt, params)
            with self.lock: self.ids["POLICIES"].append(pol_id)
            self.log_stat("POLICIES", "ins")
        elif op == "upd" and self.ids["POLICIES"]:
            pol_id = random.choice(self.ids["POLICIES"])
            sql = "UPDATE POLICIES SET STATUS = ? WHERE POLICY_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (random.choice(["Active", "Expired", "Cancelled"]), pol_id))
            self.log_stat("POLICIES", "upd")
        elif op == "del" and self.ids["POLICIES"]:
            pol_id = random.choice(self.ids["POLICIES"])
            sql = "DELETE FROM POLICIES WHERE POLICY_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (pol_id,))
            with self.lock: self.ids["POLICIES"].remove(pol_id)
            self.log_stat("POLICIES", "del")

    def generate_claim(self, conn, op):
        if not self.ids["POLICIES"] and op != "del": return
        if op == "ins":
            claim_id = random.randint(1, 10**9)
            pol_id = random.choice(self.ids["POLICIES"])
            sql = "INSERT INTO CLAIMS (CLAIM_ID, POLICY_ID, CLAIM_DATE, INCIDENT_DATE, CLAIM_AMOUNT, PAID_AMOUNT, STATUS, DESCRIPTION) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            stmt = ibm_db.prepare(conn, sql)
            incident = self.fake.date_between(start_date='-1y', end_date='today')
            claim_date = incident + timedelta(days=random.randint(1, 30))
            amt = round(random.uniform(100, 20000), 2)
            params = (claim_id, pol_id, claim_date, incident, amt, 0, "Pending", self.fake.sentence())
            ibm_db.execute(stmt, params)
            with self.lock: self.ids["CLAIMS"].append(claim_id)
            self.log_stat("CLAIMS", "ins")
        elif op == "upd" and self.ids["CLAIMS"]:
            claim_id = random.choice(self.ids["CLAIMS"])
            sql = "UPDATE CLAIMS SET STATUS = ?, PAID_AMOUNT = ? WHERE CLAIM_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (random.choice(["Open", "Closed", "Denied"]), round(random.uniform(100, 20000), 2), claim_id))
            self.log_stat("CLAIMS", "upd")
        elif op == "del" and self.ids["CLAIMS"]:
            claim_id = random.choice(self.ids["CLAIMS"])
            sql = "DELETE FROM CLAIMS WHERE CLAIM_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (claim_id,))
            with self.lock: self.ids["CLAIMS"].remove(claim_id)
            self.log_stat("CLAIMS", "del")

    def generate_transaction(self, conn, op):
        if not self.ids["ACCOUNTS"] and op != "del": return
        if op == "ins":
            tx_id = random.randint(1, 10**9)
            acc_id = random.choice(self.ids["ACCOUNTS"])
            pol_id = random.choice(self.ids["POLICIES"]) if self.ids["POLICIES"] else None
            sql = "INSERT INTO TRANSACTIONS (TRANSACTION_ID, ACCOUNT_ID, POLICY_ID, AMOUNT, TRANSACTION_TYPE, DESCRIPTION) VALUES (?, ?, ?, ?, ?, ?)"
            stmt = ibm_db.prepare(conn, sql)
            params = (tx_id, acc_id, pol_id, round(random.uniform(10, 5000), 2), random.choice(["Premium Payment", "Claim Payout", "Refund"]), self.fake.sentence())
            ibm_db.execute(stmt, params)
            with self.lock: self.ids["TRANSACTIONS"].append(tx_id)
            self.log_stat("TRANSACTIONS", "ins")
        elif op == "upd" and self.ids["TRANSACTIONS"]:
            tx_id = random.choice(self.ids["TRANSACTIONS"])
            sql = "UPDATE TRANSACTIONS SET DESCRIPTION = ? WHERE TRANSACTION_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (self.fake.sentence(), tx_id))
            self.log_stat("TRANSACTIONS", "upd")
        elif op == "del" and self.ids["TRANSACTIONS"]:
            tx_id = random.choice(self.ids["TRANSACTIONS"])
            sql = "DELETE FROM TRANSACTIONS WHERE TRANSACTION_ID = ?"
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, (tx_id,))
            with self.lock: self.ids["TRANSACTIONS"].remove(tx_id)
            self.log_stat("TRANSACTIONS", "del")

    def worker(self):
        conn = self.get_connection()
        end_time = time.time() + self.duration if self.duration > 0 else float('inf')
        
        entities = ["AGENTS", "ACCOUNTS", "CUSTOMERS", "POLICIES", "CLAIMS", "TRANSACTIONS"]
        
        while time.time() < end_time:
            entity = random.choice(entities)
            op_choices = ["ins"] * self.ratios["ins"] + ["upd"] * self.ratios["upd"] + ["del"] * self.ratios["delete"]
            op = random.choice(op_choices)
            
            try:
                if entity == "AGENTS": self.generate_agent(conn, op)
                elif entity == "ACCOUNTS": self.generate_account(conn, op)
                elif entity == "CUSTOMERS": self.generate_customer(conn, op)
                elif entity == "POLICIES": self.generate_policy(conn, op)
                elif entity == "CLAIMS": self.generate_claim(conn, op)
                elif entity == "TRANSACTIONS": self.generate_transaction(conn, op)
            except Exception as e:
                # print(f"Error in worker: {e}")
                pass
                
        ibm_db.close(conn)

    def reporter(self):
        self.start_time = time.time()
        while True:
            time.sleep(5)
            elapsed = time.time() - self.start_time
            if elapsed == 0: continue
            ops_sec = self.total_ops / elapsed
            
            print("\n--- Progress Report ---")
            print(f"Total Ops: {self.total_ops} | Avg Ops/Sec: {ops_sec:.2f}")
            print(f"{'Entity':<15} | {'Ins':<6} | {'Upd':<6} | {'Del':<6}")
            print("-" * 45)
            with self.lock:
                for entity, counts in self.stats.items():
                    print(f"{entity:<15} | {counts['ins']:<6} | {counts['upd']:<6} | {counts['del']:<6}")
            print("-" * 45)

    def run(self):
        print(f"Starting load generator with {self.parallel_level} threads for {self.duration} seconds...")
        
        # Start reporter thread
        rep_thread = threading.Thread(target=self.reporter, daemon=True)
        rep_thread.start()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.parallel_level) as executor:
            futures = [executor.submit(self.worker) for _ in range(self.parallel_level)]
            concurrent.futures.wait(futures)
        
        print("\nLoad test completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DB2 Load Generator for Demo Insurance App")
    parser.add_argument("--init-db", action="store_true", help="Initialize database with schema.sql")
    parser.add_argument("--parallel", type=int, default=1, help="Number of parallel threads")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds (0 for infinite)")
    parser.add_argument("--ins", type=int, default=70, help="Ratio of inserts")
    parser.add_argument("--upd", type=int, default=20, help="Ratio of updates")
    parser.add_argument("--delete", type=int, default=10, help="Ratio of deletes")
    
    args = parser.parse_args()
    
    ratios = {
        "ins": args.ins,
        "upd": args.upd,
        "delete": args.delete
    }
    
    loader = DB2Loader(
        parallel_level=args.parallel,
        duration=args.duration,
        ratios=ratios
    )
    
    if args.init_db:
        loader.init_db()
    else:
        loader.run()
