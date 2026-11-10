#!/usr/bin/env bash
# Exporta do banco de metadados do Airflow o histórico real de execuções.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p output/evidencias
docker compose exec -T postgres psql -U airflow -d airflow --csv -c "
  select r.dag_id, r.run_id, r.run_type, r.state as estado_execucao, t.task_id, t.state as estado_tarefa,
         t.try_number as tentativas, t.pool, t.start_date as inicio, t.end_date as fim, round(t.duration::numeric, 1) as segundos
  from task_instance t join dag_run r on r.dag_id = t.dag_id and r.run_id = t.run_id
  order by t.start_date nulls last" > output/evidencias/tarefas.csv
wc -l output/evidencias/tarefas.csv
