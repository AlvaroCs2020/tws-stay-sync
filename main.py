import time
from ibStaySync import sync

def main():
    while True:
        try:
            print("Intentamos conectarnos")
            sync()
        except Exception as e:
            print(f"[ERROR] Fallo en sync: {e}")
        print("Hubo un problema con la conexion, reintentamos en 5s")
        time.sleep(5)

if __name__ == "__main__":
    main()