#!/bin/bash
set -e

if [ "$USER" != "root" ]; then
  echo "This script must be run as the root user"
  exit 1
fi

case "$1" in
  --start|--stop) ACTION="$1" ;;
  *) echo "Usage: $0 --start | --stop"; exit 1 ;;
esac

mkdir -p /var/logs/kpiMannualLogs/

cd /opt/PDA
source venv/bin/activate

if [ "$ACTION" = "--start" ]; then
  python src/api/server.py --start

elif [ "$ACTION" = "--stop" ]; then
  python src/api/server.py --stop
fi
