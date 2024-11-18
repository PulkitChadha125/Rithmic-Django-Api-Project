import asyncio

from rithmic_api.SampleMD import run_rithmic
from rithmic_api.SampleOrder import place_order


def run():
    URI = "wss://rituz00100.rithmic.com:443"
    SYSTEM_NAME = "Rithmic Test"
    USER_ID = "pulkitchadhaqwerty@gmail.com"
    PASSWORD = "UBiKMPuY"
    EXCHANGE = "CME"
    SYMBOL = "ESZ4"

    # run_rithmic(URI, SYSTEM_NAME, USER_ID, PASSWORD, EXCHANGE, SYMBOL)
    asyncio.run(place_order(uri=URI, system_name=SYSTEM_NAME, user_id=USER_ID, password=PASSWORD, exchange=EXCHANGE,
                              symbol=SYMBOL, side="B"))

if __name__ == "__main__":
    run()
