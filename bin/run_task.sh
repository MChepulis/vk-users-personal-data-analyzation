#!/bin/bash
description=$(python3 libs/tasks.py description)
echo "$description"
echo "Введите номер задачи: "
read -r task_id
default_size=$(python3 libs/tasks.py default_size "$task_id")
if [[ -n "$default_size" ]]
then
  echo "Введите size (default=$default_size)"
  read -r size
  if [[ -z "$size" ]]
  then
    size="$default_size"
  fi
fi
echo "Отфильтровать только активных пользователей\n(которые были в сети в течении последних нескольких дней)[N/y]?"
read -r active_users
if [ "x$active_users" = "xy" ];
then
  active_users=1
  echo "В течении скольки последних дней (default=20)?"
  read -r last_days
  if [[ -z "$last_days" ]]
  then
    last_days=20
  fi
else
  active_users=0
  last_days=0
fi
if [[ -z "$default_size" ]]
then
  python3 libs/tasks.py task "$task_id" "$active_users" "$last_days"
else
  python3 libs/tasks.py task "$task_id" "$size" "$active_users" "$last_days"
fi
