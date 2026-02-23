' scripts/start_hidden.vbs
Set shell = CreateObject("WScript.Shell")
cmd = "powershell -ExecutionPolicy Bypass -NoProfile -File """ & CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName) & "\start.ps1"""
shell.Run cmd, 0, False
