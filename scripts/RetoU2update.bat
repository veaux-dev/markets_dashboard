@echo off
REM === Activa el entorno virtual y ejecuta el script ===

REM cambia la ruta a la carpeta raíz del proyecto
cd /d "M:\"

REM activa el entorno virtual
call .venv\Scripts\activate.bat

REM ejecuta el script (ajusta el nombre si es distinto)
python RetoActinver_Stocks.py

REM opcional: desactiva al final
deactivate
