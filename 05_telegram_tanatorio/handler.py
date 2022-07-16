import datetime
import json
import telegram
from telegram import ParseMode
import requests
from bs4 import BeautifulSoup
import pandas as pd
import boto3
from io import StringIO 

URL_TANATORIO = "https://funerariamotadelcuervo.es"
URI_DATA = 'URI de tu csv para almacenar los datos'
MY_TOKEN = "token_proporcionado por TheBotFather" 
CHANEL_ID = "chat_id del canal"

def scrapear_tanatorio():
    """
    Esta funcion recopila la informacion de todas las muertes publicadas por 
    el tanatorio para el año actual y devuelve un dataframe con dicha informacion. 
    """
    data = pd.DataFrame()
    
    # Creo una lista con la informacion de las muertes
    r = requests.get(f"{URL_TANATORIO}/obituario")
    soup = BeautifulSoup(r.content, "lxml")
    table = soup.findAll(attrs={"class": "obit-content"})

    for row in table:
        # obtengo los datos de interes para cada fallecido
        nombre = row.findChild("h4").text.strip()
        lugar = row.findChild(attrs={"class": "obit-place"}).text.strip()
        fecha = row.findChild(attrs={"class": "obit-date"}).text.strip()
        link = (
            URL_TANATORIO + 
            row.findChild(attrs={"class": "current-obu-link"}).attrs["href"]
        )
        
        # concateno los datos de los fallecidos en un dataframe
        row_data = pd.DataFrame.from_dict(
            {
                "nombre": [nombre], 
                "lugar": [lugar], 
                "fecha": [fecha], 
                "link": [link]
            }
        )
        data = pd.concat([data, row_data], ignore_index=True)
        
    return data


def filtrar_data(data):
    """
    Esta funcion compara los fallecimientos recopilados previamente 
    con los recopilados actualmente para quedarnos solo con los nuevos 
    fallecimientos para no informar de un fallecimiento varias veces
    """
    obituarios_antiguos = pd.read_csv(URI_DATA)
    data = data.drop_duplicates().merge(
        obituarios_antiguos,
        on = obituarios_antiguos.columns.tolist(),
        how = "left",
        indicator = True
    )
    
    data = data.loc[data._merge == "left_only", data.columns != "_merge"]
    data = data.head(5)
    return data
    
def notify_telegram(data, token, chat_id):
    """
    Esta funcion recibe un dataframe y publica un mensaje para cada fila 
    del dataframe en el canal indicado.
    """
    bot = telegram.Bot(token = token)
    for row in data.itertuples(index = False):
        bot.sendMessage(
            chat_id = chat_id, 
            text = f'*{row.nombre}*\n{row.lugar}\n{row.fecha}\n\n[Ver esquela completa]({row.link})', 
            parse_mode = ParseMode.MARKDOWN
        )
            

def run(event, context):
    # recopilamos los fallecimientos
    data = scrapear_tanatorio()
    # nos quedamos unicamente con los nuevos fallecimientos
    data = filtrar_data(data)

    if len(data) > 0:
        # Mandamos mensajes para las nuevas defunciones
        notify_telegram(data, token = MY_TOKEN, chat_id = CHANEL_ID)
            
        # Se actualiza la base de datos con las nuevas defunciones
        data.to_csv(URI_DATA, mode = 'a', header = False, index = False)
    
