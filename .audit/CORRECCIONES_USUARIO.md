# CORRECCIONES ESPECÍFICAS DEL USUARIO

## Resumen de Cambios para V4

**Total de cambios a aplicar**: 6

### Tabla: reporting.horarias

| Acción | Variable | ID | Comentario |
|--------|----------|----|-----------|
| AGREGAR | `produccion_fluido_bbl` | 107 | Incluir en reporting operaciones horarias. NO existe |
| RENOMBRAR | `prod_petroleo_bbl → prod_petroleo_diaria_bpd` | 108 | Cambiar prod_petroleo_bbl por prod_petroleo_diaria_bpd |

### Tabla: reporting.mensuales

| Acción | Variable | ID | Comentario |
|--------|----------|----|-----------|
| RENOMBRAR | `total_petroleo_bbl → produccion_petroleo_acumulada_bbl` | 98 | Cambiar total_petroleo_bbl por produccion_petroleo_acumulada_bbl |
| AGREGAR | `remanent_reserves_bbl` | 130 | Calculado. Incluir en reporting.fact_operaciones_mensuales |
| AGREGAR | `prom_produccion_fluido_bbl` | 107 | Incluir en operaciones mensuales. NO existe |

### Tabla: reporting.current_values

| Acción | Variable | ID | Comentario |
|--------|----------|----|-----------|
| AGREGAR | `estado_motor_on` | 120 | Variable binaria para saber si motor está encendido. Barra verde/rojo en dashboard |

