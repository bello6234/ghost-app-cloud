#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
cd ~/attendance-bot
while true; do
  node server.js >> ~/attendance-bot/bot.log 2>&1
  sleep 3
done
