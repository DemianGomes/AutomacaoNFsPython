import imaplib
import email
import os
import json
import logging
from email.header import decode_header
from datetime import datetime
from time import sleep
from logger_config import setup_logger
import shutil
import xml.etree.ElementTree as ET

class EmailDownloaderService:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("EmailDownloaderService")
        self.logger.info("Initializing EmailDownloaderService")
        self.setup_pastas()

    # Configura as pastas de download usando os caminhos definidos em config.json
    def setup_pastas(self):
        self.logger.info("Setting up directories")
        os.makedirs(self.config["ConfiguracoesDownload"]["PastaRaiz"], exist_ok=True)
        os.makedirs(os.path.join(self.config["ConfiguracoesDownload"]["PastaRaiz"], self.config["ConfiguracoesDownload"]["PastaArquivosTemporarios"]), exist_ok=True)
        os.makedirs(os.path.join(self.config["ConfiguracoesDownload"]["PastaRaiz"], self.config["ConfiguracoesDownload"]["PastaInvalida"]), exist_ok=True)

    # Conecta ao servidor de email usando as credenciais definidas em config.json
    def conectar_ao_email(self):
        self.logger.info("Connecting to email server")
        self.mail = imaplib.IMAP4_SSL(self.config["configuracoesEmail"]["Host"], self.config["configuracoesEmail"]["Port"])
        self.mail.login(self.config["configuracoesEmail"]["Username"], self.config["configuracoesEmail"]["Password"])
        self.mail.select(self.config["configuracoesEmail"]["Folder"])

    # Procura por emails não lidos, filtrando por assunto se necessário e filtrando por anexos XML
    def pesquisar_emails(self, subject_filter=None):
        self.logger.info("Procurando por emails não lidos")
        if subject_filter:
            filters = []
            for filter in subject_filter:
                filters.append(f'(UNSEEN SUBJECT "{filter}")')
            status, messages = self.mail.search(None, ' OR '.join(filters))
        else:
            status, messages = self.mail.search(None, 'UNSEEN')
        
        if status != 'OK':
            self.logger.error("Erro ao procurar emails")
            return []

        email_ids = messages[0].split()
        self.logger.info(f"Encontrados {len(email_ids)} emails não lidos")

        # Filtra emails que possuem anexos XML
        xml_email_ids = [email_id for email_id in email_ids if self.tem_anexos_xml(email_id)]
        self.logger.info(f"Encontrados {len(xml_email_ids)} emails com anexos XML")
        return xml_email_ids

    # Verifica se um email possui um anexo XML
    def tem_anexos_xml(self, email_id):
        status, msg_data = self.mail.fetch(email_id, '(RFC822)')
        if status != 'OK':
            self.logger.error(f"Falha ao buscar o email de id: {email_id}")
            return False

        msg = email.message_from_bytes(msg_data[0][1])
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            if part.get("Content-Disposition") is None:
                continue
            filename = part.get_filename()
            if filename and filename.lower().endswith('.xml'):
                return True
        return False

    # Faz o download dos anexos de um email
    def baixar_anexos(self, email_id):
        self.logger.info(f"Baixando anexos para o email de id: {email_id}")
        try:
            status, msg_data = self.mail.fetch(email_id, '(RFC822)')
            if status != 'OK':
                self.logger.error(f"Falha ao buscar o email de id: {email_id}")
                return

            msg = email.message_from_bytes(msg_data[0][1])
            subject, encoding = decode_header(msg["Subject"])[0]
            if isinstance(subject, bytes):
                subject = subject.decode(encoding if encoding else "utf-8")

            for part in msg.walk():
                if part.get_content_maintype() == "multipart":
                    continue
                if part.get("Content-Disposition") is None:
                    continue
                filename = part.get_filename()
                if filename:
                    # Pega data e hora atual
                    current_time = datetime.now().strftime("%d-%m-%Y_%H-%M")
                    # Adiciona a data e hora ao nome do arquivo (para evitar conflitos)
                    filename_with_timestamp = f"{filename.split('.')[0]}_{current_time}.{filename.split('.')[-1]}"
                    
                    temp_dir = os.path.join(self.config["ConfiguracoesDownload"]["PastaRaiz"], self.config["ConfiguracoesDownload"]["PastaArquivosTemporarios"])
                    filepath = os.path.join(temp_dir, filename_with_timestamp)
                    with open(filepath, "wb") as f:
                        f.write(part.get_payload(decode=True))
                    self.logger.info(f"Baixado o arquivo {filename_with_timestamp} para {filepath}")

                    # Faz um parse no XML pra achar o CNPJ
                    tree = ET.parse(filepath)
                    root = tree.getroot()
                    cnpj = root.find(".//emit/CNPJ").text

                    # Cria a pasta do CNPJ caso não exista
                    cnpj_dir = os.path.join(self.config["ConfiguracoesDownload"]["PastaRaiz"], cnpj)
                    os.makedirs(cnpj_dir, exist_ok=True)

                    # Move o arquivo para a pasta do CNPJ
                    new_filepath = os.path.join(cnpj_dir, filename_with_timestamp)
                    shutil.move(filepath, new_filepath)
                    self.logger.info(f"Moved {filename_with_timestamp} to {new_filepath}")
        except Exception as e:
            self.logger.error(f"Erro ao baixar os anexos para o email de id: {email_id} - {str(e)}")

    # Processa os emails
    def processar_emails(self):
        email_ids = self.pesquisar_emails()
        for email_id in email_ids:
            self.baixar_anexos(email_id)

    # Inicia o serviço
    def start(self):
        self.logger.info("Iniciando o serviço")
        while True:
            self.conectar_ao_email()
            self.processar_emails()
            
            intervalo = self.config["ConfiguracoesWorker"]["IntervaloEmHoras"]
            unidade_de_tempo = self.config["ConfiguracoesWorker"]["UnidadeDeTempo"]
            
            if unidade_de_tempo == "Horas":
                sleep_time = intervalo * 3600  # Converte horas para segundos
            elif unidade_de_tempo == "Minutos":
                sleep_time = intervalo * 60  # Converte minutos para segundos
            else:
                self.logger.error(f"Unidade de tempo desconhecida: {unidade_de_tempo}")
                return
            
            self.logger.info(f"Hibernando por {self.config['ConfiguracoesWorker']['Intervalo']} {self.config['ConfiguracoesWorker']['Unidade']}")
            sleep(sleep_time)

if __name__ == "__main__":
    with open("config.json", "r") as f:
        config = json.load(f)
    setup_logger()
    service = EmailDownloaderService(config)
    service.start()