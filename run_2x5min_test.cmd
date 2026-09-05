@echo off
REM One-command 2x5min live test + Kaggle upload (7 assets).
REM   run_2x5min_test.cmd              -> wipe local data + delete Kaggle dataset + run
REM   run_2x5min_test.cmd --keep-data  -> run without wiping anything
cd /d "%~dp0"
python run_2x5min_test.py %*
