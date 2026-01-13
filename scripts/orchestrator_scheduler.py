import time
import subprocess
from datetime import datetime

# --- CONFIGURATION ---
# List of times to run the job (24-hour format)
SCHEDULE_TIMES = ["21:00", "22:00"]

def run_job(script_name):
    print(f" [Job Trigger] Starting: {script_name}...")
    try:
        # Run python script as a subprocess
        subprocess.run(["python", f"scripts/{script_name}"], check=True)
        print(f" Job {script_name} Completed Successfully.")
    except subprocess.CalledProcessError as e:
        print(f" Job {script_name} Failed: {e}")

def main():
    print(f" Orchestrator started. Scheduled times: {', '.join(SCHEDULE_TIMES)}")
    
    while True:
        # Get current time (HH:MM)
        now = datetime.now().strftime("%H:%M")
        
        if now in SCHEDULE_TIMES:
            print(f"\n---  STARTING BATCH AT {now} ---")
            
            # Step : Validate & Process (Trino Pipeline)
            run_job("run_pipeline_hdfs.py") 
            
            print(f"--- BATCH COMPLETE FOR {now} ---\n")
            
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