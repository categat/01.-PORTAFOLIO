import pandas as pd

def cruzar_datos(gestion_campo, bases_enviadas, seriales_recuperados):
    # Cruce por PC
    gestion_campo["CRUCE"] = "NO CRUZO"
    gestion_campo.loc[gestion_campo["PC"].isin(bases_enviadas["PC"]), "CRUCE"] = "CRUZO PC"
    
    gestion_pc = pd.merge(gestion_campo,
                          bases_enviadas[["PC", "MACRO MOTIVO", "UNIDAD_DE_NEGOCIO", "FECHA_ENVIO IQE", "PRODUCTO", "TIPO BASE", "NombreArchivo"]],
                          left_on="PC", right_on="PC", how="inner")
    gestion_pc = gestion_pc.drop_duplicates(subset="PCS")
    gestion_pc.reset_index(drop=True, inplace=True)
    
    # Cruce por Cédula
    gestion_cc = gestion_campo.loc[gestion_campo["CRUCE"] == "NO CRUZO"]
    gestion_cc.loc[gestion_cc["Cédula usuario"].isin(bases_enviadas["CLIENTE_ID"]), "CRUCE"] = "CRUZO CEDULA"
    
    gestion_cc = pd.merge(gestion_cc,
                          bases_enviadas[["CLIENTE_ID", "MACRO MOTIVO", "UNIDAD_DE_NEGOCIO", "FECHA_ENVIO IQE", "PRODUCTO", "TIPO BASE", "NombreArchivo"]],
                          left_on="Cédula usuario", right_on="CLIENTE_ID", how="left")
    gestion_cc = gestion_cc.drop_duplicates(subset="PCS")
    gestion_cc.drop(columns="CLIENTE_ID", inplace=True)
    gestion_cc.reset_index(drop=True, inplace=True)
    
    # Validar lógica de fechas
    gestion_cc['Diferencia_Dias'] = (gestion_cc['Fecha de cierre'] - gestion_cc['FECHA_ENVIO IQE']).dt.days
    gestion_cc['Rango_Dias'] = gestion_cc['Diferencia_Dias'].apply(lambda x: 'Si' if -30 <= x <= 60 else 'No')
    
    gestion_cc.loc[gestion_cc["Rango_Dias"] == "No", ["MACRO MOTIVO", "UNIDAD_DE_NEGOCIO", "FECHA_ENVIO IQE", "PRODUCTO", "TIPO BASE", "NombreArchivo"]] = None
    gestion_cc = gestion_cc.drop(columns=["Diferencia_Dias", "Rango_Dias"], axis=1)
    gestion_cc.reset_index(drop=True, inplace=True)
    
    # Combinar gestion_pc y gestion_cc
    gestion_campo_v3 = pd.concat([gestion_pc, gestion_cc], ignore_index=True)
    
    # Cruce con seriales recuperados
    gestion_campo_v3["CRUCE"] = "NO CRUZO"
    gestion_campo_v3.loc[gestion_campo_v3["PC"].isin(seriales_recuperados["PC"]), "CRUCE"] = "CRUZO"
    
    gestion_campo_v3 = pd.merge(gestion_campo_v3,
                                seriales_recuperados[["PC", "Serial"]],
                                left_on="PC", right_on="PC", how="left", suffixes=('', '_RECUPERADO_WODEN'))
    
    gestion_campo_v3['PCS'] = gestion_campo_v3['No. Contrato'].astype(str) + gestion_campo_v3['Cédula usuario'].astype(str) + gestion_campo_v3['Serial_RECUPERADO_WODEN'].astype(str)
    gestion_campo_v3 = gestion_campo_v3.drop_duplicates(subset="PCS")
    
    gestion_campo_v3['RECUPERADOR'] = gestion_campo_v3['Serial_RECUPERADO_WODEN'].apply(lambda x: 'WODEN' if pd.notna(x) else '')
    gestion_campo_v3.reset_index(drop=True, inplace=True)
    
    return gestion_campo_v3


