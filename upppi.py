#!/usr/bin/env python
#
# Copyright (c) 2024 Diego L. Pedro <diegolpedro@gmail.com>.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Summary:
# Upppi debe descargar datos en tiempo real de cotizaciones de la API
# de PPI y almacenar en un blob de Azure.
#
import json
import logging
import os
import sys
import traceback
from common.tools import get_azure_secret_client, get_azure_blob_client
from datetime import datetime, timezone, timedelta
from ppi_client.models.instrument import Instrument
from ppi_client.ppi import PPI


logger = logging.getLogger(__name__)    # Logger (default: WARNING)
log_file_path = "./" + "upppi" + ".log"
logging.basicConfig(filename=log_file_path, level=logging.WARNING,
                    format='%(asctime)s - %(levelname)s: %(message)s')

msgBuffer = ""                          # Buffer de mensajes para PPI
utc_minus_3 = timezone(timedelta(hours=-3))
hoy = datetime.now(tz=utc_minus_3)

# Azure Clob Config
container_name = 'ppi-cots'              # Azure Blob Conection Data
blob_name = hoy.strftime('%Y%m%dcots.csv')  # Azure Blob Conection Data
blob_client = get_azure_blob_client(container_name, blob_name)   # Azure Blob Client

# PPI API Conn
ppi = PPI(sandbox=False)


# Realtime subscription to market data
def onconnect_marketdata():
    try:
        print("\nConnected to realtime market data")

        ppi.realtime.subscribe_to_element(
            Instrument("GGAL", "ACCIONES", "A-24HS"))
        msgLine = "Date,Ticker,Settlement,Trade,Price,VolumeAmount,bid,offer, \
            OpeningPrice,MaxDay,MinDay,VolumeTotalAmount\n"

        # Append text to the append blob
        blob_client.append_block(msgLine)
    except Exception as error:
        traceback.print_exc()


def ondisconnect_marketdata():
    try:
        print("\nDisconnected from realtime market data")
    except Exception as error:
        traceback.print_exc()


# Realtime MarketData
def onmarketdata(data):
    global msgBuffer  # Declare msgBuffer as global
    try:
        msg = json.loads(data)
        if msg["Trade"]:
            msgLine = f"{msg['Date']},{msg['Ticker']},{msg['Settlement']}, \
                {msg['Trade']},{msg['Price']},{msg['VolumeAmount']},,,,,,\n"
        else:
            if len(msg['Bids']) > 0:
                bid = msg['Bids'][0]['Price']
            else:
                bid = 0

            if len(msg['Offers']) > 0:
                offer = msg['Offers'][0]['Price']
            else:
                offer = 0
            msgLine = f"{msg['Date']},{msg['Ticker']},{msg['Settlement']}, \
                {msg['Trade']},,,{bid},{offer},{msg['OpeningPrice']}, \
                {msg['MaxDay']},{msg['MinDay']},{msg['VolumeTotalAmount']}\n"

        # Se utiliza un buffer de aprox ~5min
        msgBuffer = msgBuffer + msgLine
        # print(msg['Date'], len(msgBuffer))
        if len(msgBuffer) > (2560*10):
            # Append text to the append blob
            blob_client.append_block(msgBuffer)
            msgBuffer = ""

    except Exception as error:
        print(datetime.now())
        traceback.print_exc()


if __name__ == '__main__':

    logger.info(f'Iniciando upppi')

    # Azure Clob Secrets
    azs_client = get_azure_secret_client()

    # Connect to the API
    ppi_key = azs_client.get_secret('ppi-key').value
    ppi_secret = azs_client.get_secret('ppi-secret').value
    ppi.account.login_api(ppi_key, ppi_secret)

    ppi.realtime.connect_to_market_data(
        onconnect_marketdata, ondisconnect_marketdata, onmarketdata)

    # Starts connections to real time: for example to account or market data
    try:
        ppi.realtime.start_connections()
    except Exception as e:
        logger.error(f"Ocurrió un error: {e}")
        sys.exit(1)

    logger.info(f'Cerrando upppi')
