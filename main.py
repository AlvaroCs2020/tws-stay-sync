import time
import subprocess
import psutil
import os
import sys
from datetime import datetime
from TelegramBot import TelegramBot

SCRIPT_NAME = "ibStaySync.py"
CHECK_INTERVAL = 30  # segundos
HEARTBEAT_FILE = "heartbeat.txt"
TIMEOUT = 120  # segundos


def is_windows():
    return os.name == 'nt'


def start_script():
    print("Starting script...")
    if is_windows():
        # Windows
        subprocess.Popen(
            f'start "" cmd.exe /c "python {SCRIPT_NAME}"',
            shell=True,
            cwd=os.path.dirname(os.path.abspath(SCRIPT_NAME))
        )
    else:
        # Linux
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
                print("ERROR: No se pudo abrir una terminal (gnome-terminal/xterm no encontrados).")
                TelegramBot.send_message("*[WATCHER ERROR]* No se pudo iniciar terminal para ejecutar el script.")


def kill_script():
    print("Killing script process(es)...")
    for proc in psutil.process_iter(['name', 'cmdline']):
        try:
            if proc.info['cmdline']:
                command_line = " ".join(proc.info['cmdline'])
                if SCRIPT_NAME in command_line:
                    if (is_windows() and "python.exe" in proc.info['name']) or \
                       (not is_windows() and "python3" in proc.info['name']):
                        print(f"Killing PID {proc.pid}")
                        proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue


def heartbeat_expired():
    if not os.path.exists(HEARTBEAT_FILE):
        print("Heartbeat file does not exist, creando uno nuevo...")
        open(HEARTBEAT_FILE, "w").close()
        return False
    last_mod = os.path.getmtime(HEARTBEAT_FILE)
    expired = (time.time() - last_mod) > TIMEOUT
    print(f"Heartbeat vencido? {expired}")
    return expired


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
            print("Chequeando heartbeat...")
            try:
                if heartbeat_expired():
                    print("[WATCHER] Heartbeat vencido. Reiniciando script...")
                    TelegramBot.send_message("Se colgó el programa *SE LO REINICIA*")
                    kill_script()
                    time.sleep(10)
                    start_script()
            except Exception as e:
                print(f"[WATCHER ERROR] Falló al chequear el heartbeat: {e}")
                TelegramBot.send_message("*[WATCHER ERROR]* se requiere revisión")

        time.sleep(CHECK_INTERVAL / 10)