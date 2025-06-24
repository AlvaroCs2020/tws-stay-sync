import time
import subprocess
import signal
import os
from ibStaySync import sync

def main():
    process = None

    def kill_process():
        nonlocal process
        if process and process.poll() is None:
            print("[INFO] Terminando subproceso...")
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)  # Mata a todo el grupo

    try:
        while True:
            try:
                print("Intentamos conectarnos")
                # Iniciar .bat en nuevo grupo de procesos
                process = subprocess.Popen(
                    ["cmd.exe", "/c", "C:\\IBC\\StartTWS.bat"],
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                )

                time.sleep(60)  # o reemplazá por sync()
                sync()

            except Exception as e:
                print(f"[ERROR] Fallo durante la ejecución: {e}")
            finally:
                kill_process()

            print("Reintentamos en 5 segundos...\n")
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n[INTERRUPT] Ctrl+C recibido. Cerrando todo...")
        kill_process()

if __name__ == "__main__":
    main()