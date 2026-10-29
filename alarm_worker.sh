#!/data/data/com.termux/files/usr/bin/bash

URL="http://127.0.0.1:4545"
FLAG_FILE="/data/data/com.termux/files/home/attendance-bot/.alarm-ringing"
LOG="/data/data/com.termux/files/home/attendance-bot/alarm.log"

touch "$FLAG_FILE"
: > "$LOG"

while [ -f "$FLAG_FILE" ]; do
  echo "[$(date '+%H:%M:%S')] launching $URL" >> "$LOG"

  # Use Android's real am, NOT Termux's shim.
  # -f 0x10000000 = FLAG_ACTIVITY_NEW_TASK (required for non-Activity context)
  # --user 0      = current user
  /system/bin/am start \
    -a android.intent.action.VIEW \
    -d "$URL" \
    -f 0x10000000 \
    --user 0 \
    >> "$LOG" 2>&1

  termux-vibrate -d 2000 -f
  sleep 2.5
done
