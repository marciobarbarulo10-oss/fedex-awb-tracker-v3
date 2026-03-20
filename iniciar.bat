@echo off
chcp 65001 >nul
title FedEx Tracker

echo.
echo ===============================================
echo   FEDEX TRACKER — Iniciando...
echo ===============================================
echo.

:: Inicia o Python em background (sem janela)
start "" pythonw fedex_api_oficial.py

:: Aguarda 4 segundos para o servidor subir
echo  Aguardando servidor iniciar...
timeout /t 4 >nul

echo  Abrindo dashboard local...
start "" "http://localhost:8888"

echo.
echo ===============================================
echo   TUNEL NGROK — Compartilhe o link abaixo
echo ===============================================
echo.
echo  Copie o link "Forwarding" e envie para
echo  quem precisa acessar de fora da rede.
echo.
echo  Exemplo: https://procomedy-attingent-krysten.ngrok-free.dev
echo.
echo  Feche esta janela para ENCERRAR o tracker.
echo ===============================================
echo.

"C:\Users\logistica\AppData\Local\Microsoft\WindowsApps\ngrok.exe" http 8888
