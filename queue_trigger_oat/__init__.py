import logging
import azure.functions as func
#Brandon Modificación Blob Storage y Queue
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from ..shared_code.models.oat import BlobMessage
# Finaliza modificacion

from requests.exceptions import HTTPError
from ..shared_code.models.oat import OATQueueMessage

from ..shared_code.services.oat_service import get_search_data

from shared_code import utils, configurations
from shared_code.exceptions import GeneralException
from shared_code.data_collector import LogAnalytics
from shared_code.transform_utils import transform_oat_log

WORKSPACE_ID = configurations.get_workspace_id()
WORKSPACE_KEY = configurations.get_workspace_key()
API_TOKENS = configurations.get_api_tokens()
XDR_HOST_URL = configurations.get_xdr_host_url()
OAT_LOG_TYPE = configurations.get_oat_log_type()
# Queue connection
STORAGE_CONNECTION_STRING = configurations.get_storage_connection_string()

def _transfrom_logs(clp_id, detections, raw_logs):
    maps = {detection['uuid']: detection for detection in detections}
    for log in raw_logs:
        if log['uuid'] in maps:
            maps[log['uuid']].update(log)

    return [transform_oat_log(clp_id, log) for log in maps.values()]


def main(msg: func.QueueMessage) -> None:
    try:
        logging.info(f'Mensaje entro a la función')
        logging.info(f'Mensaje recibido: {str(msg.get_body())}.')
        byte_string = str(msg.get_body())
        blob_string = byte_string.decode('utf-8')
        blob_message = json.loads(blob_string)
        print(blob_message)
        blname = BlobMessage.parse_raw(blob_string) 
        print(blname.blob_name)
        blob = BlobClient.from_connection_string(conn_str=STORAGE_CONNECTION_STRING, container_name="message-container", blob_name=blname.blob_name)
        blobFile = blob.download_blob()
        # llega un listado de OATQueueMessage
        msg = blobFile.readall().decode("utf-8")
        oat_list = json.loads(msg)
        for oat in oat_list:
            message = OATQueueMessage.parse_raw(oat)
            clp_id = message.clp_id
            detections = message.detections
            post_data = message.post_data
            # Borra el Blob
            #blob.delete_blob()
            # Termina de Borrar el Blob
            token = utils.find_token_by_clp(clp_id, API_TOKENS)

            if not token:
                raise GeneralException(f'Token not found for clp: {clp_id}')

            # get workbench detail
            raw_logs = get_search_data(token, post_data)

            # transform data
            transfromed_logs = _transfrom_logs(clp_id, detections, raw_logs)

            # send to log analytics
            log_analytics = LogAnalytics(WORKSPACE_ID, WORKSPACE_KEY, OAT_LOG_TYPE)
            log_analytics.post_data(transfromed_logs)
            print(f'Send oat data successfully. count: {len(transfromed_logs)}.')

        # for peeked_message in queue_messages:
        #     print("Peeked message: " + peeked_message.content)
        #     blob_list = json.loads(peeked_message.content)
        #     for blob_message in blob_list:
        #         print(json.loads(blob_message))
        #         blname = BlobMessage.parse_raw(blob_message) 
        #         print(blname.blob_name)
        #         blob = BlobClient.from_connection_string(conn_str=STORAGE_CONNECTION_STRING, container_name="message-container", blob_name=blname.blob_name)
        #         blobFile = blob.download_blob()
        #         # llega un listado de OATQueueMessage
        #         msg = blobFile.readall().decode("utf-8")
        #         oat_list = json.loads(msg)
        #         for oat in oat_list:
        #             message = OATQueueMessage.parse_raw(oat)
        #             clp_id = message.clp_id
        #             detections = message.detections
        #             post_data = message.post_data
        #             # Borra el Blob
        #             #blob.delete_blob()
        #             # Termina de Borrar el Blob
        #             token = utils.find_token_by_clp(clp_id, API_TOKENS)

        #             if not token:
        #                 raise GeneralException(f'Token not found for clp: {clp_id}')

        #             # get workbench detail
        #             raw_logs = get_search_data(token, post_data)

        #             # transform data
        #             transfromed_logs = _transfrom_logs(clp_id, detections, raw_logs)

        #             # send to log analytics
        #             log_analytics = LogAnalytics(WORKSPACE_ID, WORKSPACE_KEY, OAT_LOG_TYPE)
        #             log_analytics.post_data(transfromed_logs)
        #             print(f'Send oat data successfully. count: {len(transfromed_logs)}.')


        # # clp_id = message.clp_id
        # # detections = message.detections
        # # post_data = message.post_data

        # # token = utils.find_token_by_clp(clp_id, API_TOKENS)

        # # if not token:
        # #     raise GeneralException(f'Token not found for clp: {clp_id}')

        # # # get workbench detail
        # # raw_logs = get_search_data(token, post_data)

        # # # transform data
        # # transfromed_logs = _transfrom_logs(clp_id, detections, raw_logs)

        # # # send to log analytics
        # # log_analytics = LogAnalytics(WORKSPACE_ID, WORKSPACE_KEY, OAT_LOG_TYPE)
        # # log_analytics.post_data(transfromed_logs)
        # # logging.info(f'Operación terminada con éxito.')

    except HTTPError as e:
        logging.exception(f'Fail to get search data! Exception: {e}')
        raise
    except:
        logging.exception('Internal error.')
        raise
