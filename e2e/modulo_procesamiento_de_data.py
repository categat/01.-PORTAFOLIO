import pandas as pd
from datetime import datetime
def procesar_gestion_campo(df):
    # Convertir la columna 'Fecha de cierre' y 'Hora' a tipo datetime
    df['Fecha de cierre'] = pd.to_datetime(df['Fecha de cierre'])
    df['Hora'] = pd.to_datetime(df['Hora'], format='%H:%M:%S').dt.time

    # Filtrar el DataFrame para incluir solo las filas donde la fecha de cierre sea igual o mayor a febrero 1 a abril 30
    df = df[(df["Fecha de cierre"] >= '2024-02-01') & (df["Fecha de cierre"] <= '2024-04-30')]

    # Crear un diccionario de prioridades para el ordenamiento de 'Prioridad'
    prioridades = {'RECUPERADO': 1, 'DESCARTADO': 2, 'GESTIONADO': 3}
    df['dicc_prioridades'] = df['Prioridad'].map(prioridades)

    # Ordenar el DataFrame por las columnas requeridas
    df = df.sort_values(by=['dicc_prioridades', 'Fecha de cierre', 'Hora'], ascending=[True, False, False]).reset_index(drop=True)

    # Eliminar duplicados basados en la columna 'PCS'
    df = df.drop_duplicates(subset='PCS').reset_index(drop=True)

    return df

def procesar_bases_enviadas(df):
    # Convertir la columna 'FECHA_ENVIO IQE' a tipo datetime
    df["FECHA_ENVIO IQE"] = pd.to_datetime(df["FECHA_ENVIO IQE"])

    # Filtrar el DataFrame para incluir solo las filas donde la fecha de envío sea igual o mayor a febrero 1 a abril 30
    df = df[(df["FECHA_ENVIO IQE"] >= '2024-02-01') & (df["FECHA_ENVIO IQE"] <= '2024-04-30')]

    # Eliminar duplicados según la columna 'CC-S'
    df = df.drop_duplicates(subset='CC-S', keep='first').reset_index(drop=True)

    return df

def procesar_seriales_recuperados(df):
    # Convertir la columna 'Fecha' al formato datetime
    df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')

    # Filtrar el DataFrame para incluir solo las filas donde la fecha de cierre sea igual o mayor a febrero 1 a abril 30
    df = df[(df['Fecha'] >= '2024-02-01') & (df['Fecha'] <= '2024-04-30')].reset_index(drop=True)

    return df

def procesar_canales_alternos(df):
    # Convertir la columna 'FECHA' al formato datetime
    df['FECHA'] = pd.to_datetime(df['FECHA'], format='%d/%m/%Y')

    # Filtrar los datos según el rango de fechas
    df = df[(df['FECHA'] >= '2024-02-01') & (df['FECHA'] <= '2024-04-30')]

    # Ordenar y eliminar duplicados
    df = df.sort_values(by=['FECHA'], ascending=False).drop_duplicates().reset_index(drop=True)

    return df
