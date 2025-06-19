import time
import subprocess
from ibStaySync import sync
def main():
    while True:
        process = None
        try:
            print("Intentamos conectarnos")

            # Ejecutar el archivo .bat
            process = subprocess.Popen(["cmd.exe", "/c", "C:\IBC\StartTWS.bat"])

            # Tu lógica principal
            # Por ejemplo:
            time.sleep(60)  # Reemplazá esto por tu `sync()` u otra lógica
            sync()
            # Terminar el proceso si sigue corriendo
            if process and process.poll() is None:
                process.terminate()
                print("[INFO] Subproceso .bat terminado correctamente")

        except Exception as e:
            print(f"[ERROR] Fallo general: {e}")
            if process and process.poll() is None:
                process.terminate()
                print("[WARN] Subproceso .bat terminado por excepción")

        print("Reintentamos en 5 segundos...\n")
        time.sleep(5)

if __name__ == "__main__":
    main()