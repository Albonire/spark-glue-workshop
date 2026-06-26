# Guía de Evidencias del Taller

Este documento describe qué capturas de pantalla se necesitan para cada paso del taller.

## Pasos completados (requieren captura del usuario)

### step_0_budget.png — Budget AWS
- Ir a AWS Console → Budgets
- Buscar `spark-glue-workshop-budget`
- Capturar pantalla mostrando: Budget name, Amount ($2.00), Status (Healthy)

### step_2_s3_bucket.png — Bucket S3 estructurado
- Ir a AWS Console → S3 → `spark-glue-workshop-datalake-af-01`
- Capturar pantalla mostrando las 5 carpetas:
  - `athena-results/`
  - `bronze/` (con 2 CSVs dentro)
  - `gold/`
  - `silver/`
  - `temp/`

## Pasos pendientes (ejecutar en AWS Console y capturar)

### step_5_glue_silver_succeeded.png — Job Bronze→Silver
1. Ir a AWS Glue Studio → Jobs
2. Crear job `spark-glue-workshop-bronze-to-silver`:
   - IAM Role: `spark-glue-workshop-role`
   - Script: `s3://spark-glue-workshop-datalake-af-01/scripts/job_bronze_to_silver.py`
   - Glue 4.0, G.1X, 2 workers
   - Job parameter: `--BUCKET` = `spark-glue-workshop-datalake-af-01`
3. Ejecutar job → Esperar estado **Succeeded**
4. Capturar pantalla del job run con estado Succeeded y logs

### step_6_glue_gold_succeeded.png — Job Silver→Gold
1. Ir a AWS Glue Studio → Jobs
2. Crear job `spark-glue-workshop-silver-to-gold`:
   - IAM Role: `spark-glue-workshop-role`
   - Script: `s3://spark-glue-workshop-datalake-af-01/scripts/job_silver_to_gold.py`
   - Glue 4.0, G.1X, 2 workers
   - Job parameter: `--BUCKET` = `spark-glue-workshop-datalake-af-01`
3. Ejecutar job → Esperar estado **Succeeded**
4. Capturar pantalla del job run con estado Succeeded y logs

### step_7_step_functions.png — State Machine
1. Ir a AWS Step Functions → State machines
2. Crear `spark-glue-workshop-orchestrator` usando `step-functions/state_machine.json`
3. Ejecutar state machine → Esperar **Succeeded**
4. Capturar pantalla del execution graph mostrando ambos steps verdes

### step_8_athena_queries.png — Consultas Athena
1. Ir a Athena → Query editor
2. Settings → Manage → Output location: `s3://spark-glue-workshop-datalake-af-01/athena-results/`
3. Ejecutar `sql/create_athena_tables.sql` (reemplazar `<BUCKET>`)
4. Ejecutar una query de `sql/business_questions.sql` (ej. P1)
5. Capturar pantalla con resultados de la query

## Comandos de verificación CLI

```bash
# Verificar bucket S3
aws s3 ls s3://spark-glue-workshop-datalake-af-01/

# Verificar CSVs en bronze
aws s3 ls s3://spark-glue-workshop-datalake-af-01/bronze/

# Verificar IAM role
aws iam get-role --role-name spark-glue-workshop-role

# Verificar Glue jobs
aws glue get-jobs

# Verificar Step Functions
aws stepfunctions list-state-machines

# Verificar Athena
aws athena list-databases --catalog-name AwsDataCatalog
```