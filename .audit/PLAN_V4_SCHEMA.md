# PLAN DE CREACIÓN V4 REPORTING SCHEMA

## Filosofía Zero-Calc / Zero-Hardcode

- ✅ **Zero-Calc**: BI no calcula, solo visualiza
- ✅ **Zero-Hardcode**: No números mágicos en código
- ✅ **Referencial como cerebro**: Única fuente de verdad

## Cambios a Implementar

**Total de modificaciones**: 6

### Variables a Agregar/Renombrar por Tabla

#### horarias
- [ ] AGREGAR: produccion_fluido_bbl
- [ ] RENOMBRAR: prod_petroleo_diaria_bpd

#### mensuales
- [ ] RENOMBRAR: produccion_petroleo_acumulada_bbl
- [ ] AGREGAR: remanent_reserves_bbl
- [ ] AGREGAR: prom_produccion_fluido_bbl

#### current_values
- [ ] AGREGAR: estado_motor_on

