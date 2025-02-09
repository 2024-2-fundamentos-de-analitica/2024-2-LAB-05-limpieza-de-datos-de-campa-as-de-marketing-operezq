"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import zipfile
import glob
import os

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    datas = []

    for directorio in os.listdir("files/input"):
        with zipfile.ZipFile(os.path.join("files/input",directorio), 'r') as archivo:
            for csv in archivo.namelist():
                with archivo.open(csv) as file:
                    df = pd.read_csv(file, sep=",", encoding="utf-8")
                    datas.append(df)

    df = pd.concat(datas, ignore_index=True)

    cliente = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    cliente['job'] = cliente['job'].str.replace('.', '').str.replace('-', '_')
    cliente['education'] = cliente['education'].str.replace('.', '_').replace('unknown', pd.NA)
    cliente['credit_default'] = cliente['credit_default'].map({'yes': 1}).fillna(0)
    cliente['mortgage'] = cliente['mortgage'].map({'yes': 1}).fillna(0)
    cliente.to_csv('files/output/client.csv',index=False)

    campaign = df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'day', 'month']].copy()
    campaign['previous_outcome'] = campaign['previous_outcome'].apply(lambda x: 1 if x == "success" else 0)
    campaign['campaign_outcome'] = campaign['campaign_outcome'].apply(lambda x: 1 if x == "yes" else 0)
    campaign['last_contact_date'] = pd.to_datetime('2022-' + campaign['month'] + '-' + campaign['day'].astype(str), format='%Y-%b-%d').dt.strftime('%Y-%m-%d')
    campaign.drop(columns=['month', 'day'], inplace=True)
    campaign.to_csv('files/output/campaign.csv',index=False)

    economics = df[['client_id','cons_price_idx','euribor_three_months']]
    economics.to_csv('files/output/economics.csv',index=False)

    print(df)
if __name__ == "__main__":
    clean_campaign_data()
    