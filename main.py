import subprocess
import time
import psutil
import os
from datetime import datetime
from TelegramBot import TelegramBot

SCRIPT_NAME = "ibStaySync.py"  # Tu script Python
CHECK_INTERVAL = 30  # segundos
HEARTBEAT_FILE = "heartbeat.txt"
TIMEOUT = 120  # segundos

def is_running():
    if os.path.exists(HEARTBEAT_FILE):
        os.remove(HEARTBEAT_FILE)
    return False

def start_script():
    print("starting script (Linux)")
    
    # Usa gnome-terminal o xterm para abrir una terminal visible
    try:
        subprocess.Popen([
            "gnome-terminal", "--", "bash", "-c", f"python3 {SCRIPT_NAME}; exec bash"
        ])
    except FileNotFoundError:
        try:
            subprocess.Popen([
                "xterm", "-e", f"python3 {SCRIPT_NAME}"
            ])
        except FileNotFoundError:
            print("ERROR: No se pudo abrir una terminal. Asegurate de tener 'gnome-terminal' o 'xterm' instalado.")
            TelegramBot.send_message("*[WATCHER ERROR]* No se pudo iniciar terminal para ejecutar el script.")

def kill_script():
    print("Killing script process(es)...")
    for proc in psutil.process_iter(['name', 'cmdline']):
        try:
            if 'python3' in proc.info['name'] and proc.info['cmdline']:
                if SCRIPT_NAME in " ".join(proc.info['cmdline']):
                    print(f"Killing PID {proc.pid}")
                    proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

def heartbeat_expired():
    if not os.path.exists(HEARTBEAT_FILE):
        print("Heartbeat file does not exist")
        open(HEARTBEAT_FILE, "w").close()
        return False
    last_mod = os.path.getmtime(HEARTBEAT_FILE)
    print(f"resultado {(time.time() - last_mod) > TIMEOUT}")
    return (time.time() - last_mod) > TIMEOUT

if __name__ == "__main__":
    start_script()
    time.sleep(CHECK_INTERVAL)
    last_checked_minute = -1
    open(HEARTBEAT_FILE, "w").close()

    while True:
        now = datetime.now()
        print("Esperando...")
        if now.minute % 2 == 0 and now.minute != last_checked_minute:
            last_checked_minute = now.minute
            print("Sigue vivo?")
            try:
                if heartbeat_expired():
                    print("[WATCHER] Heartbeat vencido. Reiniciando script...")
                    TelegramBot.send_message("Se colgó el programa *SE LO REINICIA*")
                    kill_script()
                    time.sleep(10)
                    start_script()
            except Exception as e:
                print(f"[WATCHER ERROR] Falló al chequear el heartbeat: {e}")
                TelegramBot.send_message(f"*[WATCHER ERROR]* se requiere revisión")

        time.sleep(CHECK_INTERVAL / 10)
