import os
import psutil
import subprocess
import time


def is_process_running(exe_name):
    """Check if there is any running process that contains the given exe name."""
    for proc in psutil.process_iter(["pid", "name"]):
        try:
            if proc.info["name"] == exe_name:
                return True, proc.info["pid"]
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return False, None


def kill_process(pid):
    """Kill the process with the given pid."""
    try:
        p = psutil.Process(pid)
        p.terminate()
        p.wait(timeout=3)
    except psutil.NoSuchProcess:
        pass
    except psutil.TimeoutExpired:
        p.kill()


def run_exe(filepath):
    """Run the .exe file and wait for it to finish."""
    process = subprocess.Popen(filepath)
    process.wait()


def main():
    exe_name = "interceptor.exe"
    exe_path = os.path.join(os.getcwd(), exe_name)

    while True:
        running, pid = is_process_running(exe_name)
        if running:
            print(
                f"Found running instance of {exe_name} with PID {pid}. Terminating it."
            )
            kill_process(pid)

        print(f"Starting {exe_name}")
        run_exe(exe_path)
        print(f"{exe_name} finished. Restarting...")

        # Optional: add a small delay if needed
        time.sleep(1)


if __name__ == "__main__":
    main()
