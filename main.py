import subprocess
import time
import sys
from ibStaySync import sync
def main():
    while True:
        print("Intentamos conectarnos")
        sync()
        print("Hubo un problema con la conexion")


if __name__ == "__main__":
    main()