import time
import subprocess
import signal
import os
from ibStaySync import sync
from ibStaySync import get_sync

def main():
    process = None

    def kill_process():
        print("[WARN] Se decidio cerrar TWS")

        send_command_path = r"C:\IBC\SendCommand.bat"
        working_dir = r"C:\IBC"

        #subprocess.run([send_command_path, "STOP"], cwd=working_dir, shell=True)
        time.sleep(5)
    try:
        while True:
            try:
                print("Intentamos conectarnos")
                # Iniciar .bat en nuevo grupo de procesos
                # process = subprocess.Popen(
                #     ["cmd.exe", "/c", "C:\\IBC\\StartTWS.bat"],
                #     creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                # )
                #
                # time.sleep(60)  # o reemplazá por sync()
                sync()
                # get_sync()

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