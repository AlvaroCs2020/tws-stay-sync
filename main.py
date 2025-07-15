import time
import subprocess
import time
import psutil
import os
from datetime import datetime
from TelegramBot import TelegramBot
SCRIPT_NAME = "ibStaySync.py"  # Cambialo si tu script tiene otro nombre
CHECK_INTERVAL = 30  # segundos
HEARTBEAT_FILE = "heartbeat.txt"
TIMEOUT = 120
def is_running():
    if os.path.exists(HEARTBEAT_FILE):
        os.remove(HEARTBEAT_FILE)
    return False

def start_script():
    print("starting script")
    subprocess.Popen(
        f'start "" cmd.exe /c "python {SCRIPT_NAME}"',
        shell=True,
        cwd=os.path.dirname(os.path.abspath(SCRIPT_NAME))
    )
def kill_script():
    print("Killing script process(es)...")
    for proc in psutil.process_iter(['name', 'cmdline']):
        try:
            if proc.info['name'] == "python.exe" and proc.info['cmdline']:
                if SCRIPT_NAME in " ".join(proc.info['cmdline']):
                    print(f"Killing PID {proc.pid}")
                    proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
def heartbeat_expired():
    if not os.path.exists(HEARTBEAT_FILE):
        print("Heartbeat file does not exist")
        open("heartbeat.txt", "w").close()
        return False  # heartbeat fue borrado, t0do bien
    # si existe pero es viejo (> TIMEOUT), asumo que se colgó
    last_mod = os.path.getmtime(HEARTBEAT_FILE)
    print(f"resultado {(time.time() - last_mod) > TIMEOUT}")
    return (time.time() - last_mod) > TIMEOUT


if __name__ == "__main__":
    start_script()
    time.sleep(CHECK_INTERVAL)

    last_checked_minute = -1  # para evitar doble chequeo en el mismo minuto
    open("heartbeat.txt", "w").close()
    while True:
        now = datetime.now()
        print("Esperando")
        # Ejecutamos solo si el minuto cambió y es múltiplo de 10 (una vez cada 10 min exactos)
        if now.minute % 2 == 0 and now.minute != last_checked_minute:
            last_checked_minute = now.minute
            print("Sigue vivo?")
            try:
                if heartbeat_expired():
                    print("[WATCHER] Heartbeat vencido. Reiniciando script...")
                    TelegramBot.send_message(
                        f"Se colgo el programa *SE LO REINICIA*")
                    kill_script()
                    time.sleep(10)
                    start_script()
            except Exception as e:
                print(f"[WATCHER ERROR] Falló al chequear el heartbeat: {e}")
                TelegramBot.send_message(
                    f"*[WATCHER ERROR]* se requiere revision")

        time.sleep(CHECK_INTERVAL/10)