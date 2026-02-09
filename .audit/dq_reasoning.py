import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

def analyze_dq_null_logic():
    print("--- ANÁLISIS TÉCNICO: NULOS EN stage.tbl_pozo_scada_dq ---")
    
    with engine.connect() as conn:
        try:
            # 1. Ver nulos en la tabla de resultados
            dq_res = pd.read_sql("SELECT nombre_columna, valor_leido, valor_esperado_minimo, valor_esperado_maximo, resultado_dq FROM stage.tbl_pozo_scada_dq", conn)
            
            # 2. Ver reglas en referencial
            rules = pd.read_sql("SELECT nombre_columna, valor_min, valor_max FROM referencial.tbl_dq_rules", conn)
            
            print("\n[RESULTADOS DQ EN STAGE]")
            print(dq_res.to_string(index=False))
            
            print("\n[REGLAS DEFINIDAS EN REFERENCIAL]")
            # Filtrar solo las columnas que aparecen en los resultados para comparar
            cols_in_res = dq_res['nombre_columna'].unique()
            print(rules[rules['nombre_columna'].isin(cols_in_res)].to_string(index=False))
            
            # 3. Explicación automática
            print("\n[DIAGNÓSTICO AUTOMÁTICO]")
            for col in cols_in_res:
                rule_row = rules[rules['nombre_columna'] == col].iloc[0]
                res_row = dq_res[dq_res['nombre_columna'] == col].iloc[0]
                
                if pd.isna(res_row['valor_esperado_minimo']) and pd.isna(rule_row['valor_min']):
                    print(f"  - {col}: El nulo en 'minimo' es ESPERADO (No se definió valor_min en Referencial).")
                elif pd.isna(res_row['valor_esperado_maximo']) and pd.isna(rule_row['valor_max']):
                    print(f"  - {col}: El nulo en 'maximo' es ESPERADO (No se definió valor_max en Referencial).")
                else:
                    print(f"  - {col}: Hay una DISCREPANCIA (Regla tiene límites pero Resultado no).")
                    
        except Exception as e:
            print(f"   [ERROR]: {e}")

if __name__ == "__main__":
    analyze_dq_null_logic()
