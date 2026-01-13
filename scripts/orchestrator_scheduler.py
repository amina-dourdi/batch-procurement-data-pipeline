import time
import subprocess
from datetime import datetime

# Define the schedule
SCHEDULE_TIME = "14:00"

def run_job(script_name):
    print(f"⏰ [22:00] Triggering Job: {script_name}...")
    try:
        # We use subprocess to run the other python scripts like a command line
        subprocess.run(["python", f"scripts/{script_name}"], check=True)
        print(f"✅ Job {script_name} Completed Successfully.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Job {script_name} Failed: {e}")

def main():
    print(f"🚀 Orchestrator started. Waiting for {SCHEDULE_TIME} daily...")
    
    while True:
        # Get current time (HH:MM)
        now = datetime.now().strftime("%H:%M")
        
        if now == SCHEDULE_TIME:
            print(f"\n--- STARTING DAILY BATCH: {datetime.now()} ---")
            
            # Step 1: Generation (The script we created)
            run_job("generate_master_data.py")
            
            # Step 2: Processing (The SQL script - we will build this next!)
            # run_job("run_pipeline.py") 
            
            print(f"--- BATCH COMPLETE ---\n")
            
            # Sleep for 61 seconds so we don't trigger it twice in the same minute
            time.sleep(61)
            
        else:
            # Sleep for 30 seconds before checking again
            time.sleep(30)

if __name__ == "__main__":
    # Ensure print statements show up immediately in Docker logs
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    main()