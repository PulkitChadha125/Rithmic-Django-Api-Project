from rithmic_api.SampleMD import run_rithmic



def run():
    URI = "wss://rituz00100.rithmic.com:443"
    SYSTEM_NAME = "Rithmic Test"
    USER_ID = "pulkitchadhaqwerty@gmail.com"
    PASSWORD = "UBiKMPuY"
    EXCHANGE = "CME"
    SYMBOL = "ESZ4"

    run_rithmic(URI, SYSTEM_NAME, USER_ID, PASSWORD, EXCHANGE, SYMBOL)

if __name__ == "__main__":
    run()
