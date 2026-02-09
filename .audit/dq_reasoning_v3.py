import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

def analyze_dq_null_logic_v3():
    print("--- ANÁLISIS TÉCNICO V3: NULOS EN stage.tbl_pozo_scada_dq ---")
    
    with engine.connect() as conn:
        try:
            # 1. Resultados DQ (Nombres exactos de tabla V4)
            # Columnas: dq_id, variable_id, regla_id, valor_observado, valor_esperado_min, valor_esperado_max
            dq_res = pd.read_sql("SELECT variable_id, regla_id, valor_observado, valor_esperado_min, valor_esperado_max, resultado_dq FROM stage.tbl_pozo_scada_dq", conn)
            
            # 2. Reglas en referencial (Para cruce)
            rules = pd.read_sql("SELECT variable_id, rule_id, nombre_columna, valor_min, valor_max FROM referencial.tbl_dq_rules", conn)
            
            # Unir para tener nombres técnicos de las variables
            merged = pd.merge(dq_res, rules, left_on='regla_id', right_on='rule_id', how='left')
            
            print("\n[RESULTADOS DQ + REGLAS]")
            display_cols = ['nombre_columna', 'valor_observado', 'valor_esperado_min', 'valor_esperado_max', 'valor_min', 'valor_max', 'resultado_dq']
            print(merged[display_cols].to_string(index=False))
            
            # 3. Explicación
            print("\n[DIAGNÓSTICO DE NULOS]")
            for _, row in merged.iterrows():
                print(f"\n> Variable: {row['nombre_columna']}")
                
                # Chequeo Min
                if pd.isna(row['valor_esperado_min']):
                    if pd.isna(row['valor_min']):
                        print(f"  - valor_esperado_min es NULO porque la regla NO TIENE límite mínimo definido en Referencial (Solo valida Máximo).")
                    else:
                        print(f"  - valor_esperado_min es NULO: Posible error de propagación en el motor DQ.")
                
                # Chequeo Max
                if pd.isna(row['valor_esperado_max']):
                    if pd.isna(row['valor_max']):
                        print(f"  - valor_esperado_max es NULO porque la regla NO TIENE límite máximo definido en Referencial (Solo valida Mínimo).")
                    else:
                        print(f"  - valor_esperado_max es NULO: Posible error de propagación en el motor DQ.")

        except Exception as e:
            print(f"   [ERROR]: {e}")

if __name__ == "__main__":
    analyze_dq_null_logic_v3()
