#!/bin/bash
cd /home/gamma
source activate gamma
export PATH="$PATH:~/.local/bin"
echo $PATH
airflow standalone
