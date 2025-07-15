Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "cmd /k python main.py", 1, False
