#!/bin/bash

echo "Введите стартовый id пользователя (default=1): "
read -r start_id
if [[ -z "$start_id" ]]
then
  start_id=1
fi

echo "Введите конечный id пользователя (default=633228552): "
read -r end_id
if [[ -z "$end_id" ]]
then
  end_id=633228552
fi

echo "Введите шаг загрузки (default=50000): "
read -r step
if [[ -z "$step" ]]
then
  step=50000
fi

echo "Введите максимальное количество потоков загрузки (default=10): "
read -r max_workers
if [[ -z "$max_workers" ]]
then
  max_workers=10
fi

python3 libs/connector.py "$start_id" "$end_id" "$step" "$max_workers"