# Agentes expertos en Data para la auditoría BP010

Carpeta de agentes en el workspace: **`BP011-GenAI-agents`**  
Ruta absoluta: `d:\ITMeet\Operaciones\BP011-GenAI-agents`

---

## Cómo invocarlos en Cursor

1. **Por nombre en el chat**: escribe `@` y el nombre del agente (si Cursor los tiene indexados).
2. **Cargando el archivo**: abre el `.md` del agente y en el chat escribe algo como:  
   *“Actúa como el agente en el archivo abierto y [tu tarea de auditoría]”.*
3. **Referencia directa**:  
   `Usando BP011-GenAI-agents/.agents/06-data-engineering/postgres-expert.md haz [tarea].`

---

## Expertos en data recomendados para esta auditoría

Son los más alineados con pipelines ETL, PostgreSQL, calidad de datos y reporting.

| Rol | Archivo | Para qué usarlo en la auditoría |
|-----|--------|----------------------------------|
| **PostgreSQL / datos** | `.agents/06-data-engineering/postgres-expert.md` | Esquemas (stage, referencial, reporting), índices, DQ, stored procedures, integridad y rendimiento. |
| **Data science / análisis** | `.agents/06-data-engineering/data-science.md` | Calidad de datos, patrones en KPIs, validación de métricas y consistencia de transformaciones. |
| **Ingeniería inversa BI** | `.agents/06-data-engineering/dashboard-reverse-engineer.md` | Mapeo mockup → reporting, hojas de validación, DDL y lógica en BD vs dashboard. |
| **Arquitecto Data/ML** | `.agents/07-ai-machine-learning/ml-architect.md` o `data-ml-architect-v1.0.0/data-ml-architect-v1.0.0.md` | Arquitectura del pipeline, lineage, patrones (lambda/kappa), gobernanza y puntos ciegos. |
| **ML Engineer** | `.agents/07-ai-machine-learning/ml-engineer.md` | Pipelines de datos en producción, orquestación, robustez y despliegue. |

Versiones “empaquetadas” en la raíz de agentes (por si prefieres usarlas):

- `postgres-data-expert-v1.0.0/postgres-data-expert-v1.0.0.md`
- `data-science-v1.0.0/data-science-v1.0.0.md`
- `data-ml-architect-v1.0.0/data-ml-architect-v1.0.0.md`
- `ml-engineer-v1.0.0/ml-engineer-v1.0.0.md`

---

## Brief de auditoría para pegar al invocar

Puedes copiar y pegar esto (o una parte) cuando invoques a cada agente:

```text
Contexto: Auditoría técnica del repo BP010-data-pipelines (entorno de auditoría aislado en BP010-data-pipelines-auditoria).

- PostgreSQL 15 en Docker (puerto 5433), esquemas: stage, referencial, reporting, universal.
- Pipeline: landing_scada_data → pivot (V1__stage_to_stage) → tbl_pozo_produccion → DQ (tbl_pozo_scada_dq) → reporting (sp_load_to_reporting) → dataset_current_values.
- Hay dual-lambda (stage ETL + reporting ETL), notebooks locales y MASTER_PIPELINE_RUNNER.

Necesito que [según el agente: revises esquemas y DQ / valides métricas y transformaciones / mapees reporting vs dashboard / evalúes arquitectura y lineage / revises orquestación y robustez] y reportes hallazgos y recomendaciones en español.
```

Ajusta la última frase según el agente (postgres, data-science, dashboard-reverse-engineer, ml-architect, ml-engineer).

---

## Resumen de perfiles

- **postgres-expert**: Esquemas, índices, EXPLAIN, partición, JSONB, FDW, PL/pgSQL, integridad.
- **data-science**: EDA, calidad de datos, métricas, significancia, reproducibilidad, insights.
- **dashboard-reverse-engineer**: Mockups → CSV de validación y DDL, “la cocina” en BD.
- **data-ml-architect**: Arquitectura de datos y ML, pipelines end-to-end, gobernanza, coste.
- **ml-engineer**: Despliegue, latencia, throughput, monitoreo, versionado de modelos/pipelines.

Para una auditoría completa del repo, conviene usar al menos **postgres-expert** (esquemas y DQ) y **data-ml-architect** (arquitectura y lineage); el resto según si quieres foco en análisis, BI o operación.
