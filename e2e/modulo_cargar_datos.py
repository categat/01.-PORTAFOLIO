import pandas as pd

def cargar_datos():
    # Cargar y procesar el primer archivo
    df1 = pd.read_parquet("C:/Users/oolea/Documents/01. Codigos PY/E2E- B2B/001. INFO OFICIAL ACTUALIZADA JUNIO/DATA E2E/1.WFSM GESTION DE CAMPO SERIALES - CONCATENADO-E2E.parquet")
    df1['PCS'] = df1['No. Contrato'].astype(str) + df1['Cédula usuario'].astype(str) + df1['Serial'].astype(str)
    df1['PC'] = df1['No. Contrato'].astype(str) + df1['Cédula usuario'].astype(str)
    df1['PCS'] = df1['PCS'].str.upper()
    df1['PC'] = df1['PC'].str.upper()
    gestion_campo = df1.copy()

    # Cargar y procesar el segundo archivo
    df2 = pd.read_parquet("C:/Users/oolea/Documents/01. Codigos PY/E2E- B2B/001. INFO OFICIAL ACTUALIZADA JUNIO/DATA E2E/BASES ENVIADAS - HISTORICO (E2E).parquet")
    df2['PC'] = df2['PEDIDO'].astype(str) + df2['CLIENTE_ID'].astype(str)
    df2['CC-S'] = df2['CLIENTE_ID'].astype(str) + df2['SERIAL_EQUIPO'].astype(str)
    df2['PCS'] = df2['PEDIDO'].astype(str) + df2['CLIENTE_ID'] + df2['SERIAL_EQUIPO'].astype(str)
    df2['PC'] = df2['PC'].str.upper()
    df2['CC-S'] = df2['CC-S'].str.upper()
    df2['PCS'] = df2['PCS'].str.upper()
    bases_enviadas = df2.copy()

    # Cargar y procesar el tercer archivo
    df3 = pd.read_csv("C:/Users/oolea/Documents/01. Codigos PY/E2E- B2B/001. INFO OFICIAL ACTUALIZADA JUNIO/DATA E2E/3. WFSM SERIALES RECUPERADOS - ACTUAL (BASES + E2E).csv", sep=";", encoding="latin-1")
    df3["PC"] = df3["No. Contrato"].astype(str) + df3["Cédula usuario"].astype(str)
    df3["PCS"] = df3["No. Contrato"].astype(str) + df3["Cédula usuario"].astype(str) + df3["Serial"].astype(str)
    df3["PC"] = df3["PC"].str.upper()
    df3["PCS"] = df3["PCS"].str.upper()
    seriales_recuperados = df3.copy()

    # Cargar y procesar el cuarto archivo
    df4 = pd.read_csv("C:/Users/oolea/Documents/01. Codigos PY/E2E- B2B/001. INFO OFICIAL ACTUALIZADA JUNIO/DATA E2E/4.CANALES ALTERNOS - ACTUAL (BASES + E2E).csv", sep=";", encoding="latin-1")
    df4["SERIALES"] = df4["SERIALES"].str.upper()
    canales_alternos = df4.copy()

    # Cargar y procesar el quinto archivo
    df5 = pd.read_csv("C:/Users/oolea/Documents/01. Codigos PY/E2E- B2B/001. INFO OFICIAL ACTUALIZADA JUNIO/DATA E2E/5.NUEVO_CONSOLIDADO_CHURN_JUNIO_HISTORICO.txt", sep=";")
    df5["SERIAL_EQUIPO"] = df5["SERIAL_EQUIPO"].str.upper()
    churn = df5.copy()

    # Cargar y procesar el sexto archivo
    df6 = pd.read_parquet("C:/Users/oolea/Documents/01. Codigos PY/E2E- B2B/001. INFO OFICIAL ACTUALIZADA JUNIO/DATA E2E/DESCONEXIONES HISTORICO_CRUCE INVERSO.parquet")
    df6['PID'] = df6['PEDIDO'] + df6['IDUNICOSERVICIO']
    df6["PID"] = df6["PID"].str.upper()
    desconexiones = df6.copy()

    return gestion_campo, bases_enviadas, seriales_recuperados, canales_alternos, churn, desconexiones
