class Config:
    BROKER_ADDRESS:str = 'w1b0d181.ala.eu-central-1.emqxsl.com'
    BROKER_PORT:int = 8883
    CERTIFICATE_FILE:str = '../emqxsl-ca.crt'
    USERNAME:str = 'quocphu'
    PASSWORD:str = '190505'
    TOPICS:list[str] = ['TEMPERATURE', 'PRESSURE']
    BOOTSTRAP_SERVERS: list[str]= ['localhost:9092']