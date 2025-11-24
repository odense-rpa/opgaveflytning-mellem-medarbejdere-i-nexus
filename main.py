import asyncio
import logging
import sys

from datetime import datetime
from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential
from nexus_database_client import NexusDatabaseClient
from kmd_nexus_client import NexusClientManager
from xflow_client import XFlowClient, ProcessClient
from odk_tools.tracking import Tracker
from odk_tools.reporting import report

nexus: NexusClientManager
nexus_database_client: NexusDatabaseClient
xflow_client: XFlowClient
xflow_process_client: ProcessClient
tracker: Tracker

proces_navn = "Opgaveflytning mellem medarbejdere i Nexus"
logger = logging.getLogger(proces_navn)

async def populate_queue(workqueue: Workqueue):    
    xlow_søge_query = {
        "text": "OPGAVEFLYTNING MELLEM MEDARBEJDERE I NEXUS",
        "processTemplateIds": [
            "714"
        ],
        "startIndex": 0,        
        "createdDateFrom": "01-01-1980",
        "createdDateTo":  datetime.today().strftime('%d-%m-%Y'),
    }

    igangværende_processer = xflow_process_client.search_processes_by_current_activity(
        query=xlow_søge_query,
        activity_name="RPAIntegration"
    )

    for proces in igangværende_processer:        
        # TODO: Check if the process is already in the workqueue
        
        flyt_opgaver_fra_initialer: str = str(xflow_process_client.find_process_element_value(proces, "FraMedarbejder", "Tekst"))
        flyt_opgaver_til_initialer: str = str(xflow_process_client.find_process_element_value(proces, "TilMedarbejder", "Tekst"))

        medarbejder_fra = nexus.organisationer.hent_medarbejder_ved_initialer(flyt_opgaver_fra_initialer)

        if medarbejder_fra is None: 
            # Finder første (og eneste) acitivity tilhøerende RPAIntegration
            activity_id = next(activity["possibleActivitiesIdsToRejectTo"][0] for activity in proces["activities"] if activity["activityName"] == "RPAIntegration")
            xflow_process_client.reject_process(proces["publicId"], activity_id, f"Medarbejder med initialer: {medarbejder_fra} blev ikke fundet i Nexus")
            logging.warning(
                    f"Anmodning med id: {proces['publicId']} blev afvist, da medarbejder med initialer: {flyt_opgaver_fra_initialer} ikke blev fundet i Nexus."
                )
            break
            
        kø_data = {
            "from_initials": flyt_opgaver_fra_initialer,
            "to_initials": flyt_opgaver_til_initialer,
            "xflow_process_id": proces["publicId"],
        }

        workqueue.add_item(kø_data, f"{flyt_opgaver_fra_initialer} - {flyt_opgaver_til_initialer}")        

async def process_workqueue(workqueue: Workqueue):
    for item in workqueue:
        with item:
            data = item.data            
            opgaver = nexus_database_client.get_tasks_by_professional(data["from_initials"])

            til_medarbejder = None

            if data["to_initials"] is not None and not data["to_initials"].strip() == "":
                medarbejder = nexus.organisationer.hent_medarbejder_ved_initialer(data["to_initials"])
                til_medarbejder = {
                    "professionalId": medarbejder["id"],
                    "displayName": medarbejder["fullName"],
                    "displayNameWithUniqId": f"{medarbejder['fullName']} ({medarbejder['primaryIdentifier']})",
                    "active": medarbejder["active"]
                }

            for opgave in opgaver:                
                try:
                    borger = nexus.borgere.hent_borger(opgave["cpr"])

                    if borger is None:
                        raise WorkItemError(f"Kunne ikke finde borger med CPR: {opgave['cpr']}")

                    nexus_opgave = nexus.opgaver.hent_opgave_for_borger(borger, opgave["id"])

                    if nexus_opgave is None:
                        report(
                            report_id="opgaveflytning_mellem_medarbejdere_i_nexus",
                            group="Fejl",
                            json={
                                "CPR": opgave["cpr"],
                                "Fejl": "Kunne ikke finde opgave i Nexus",
                            }
                        )                        
                        continue

                    nexus_opgave["professionalAssignee"] = til_medarbejder
                    nexus.opgaver.rediger_opgave(nexus_opgave)
                    
                    tracker.track_task(proces_navn)
                except WorkItemError:
                    report(
                            report_id="opgaveflytning_mellem_medarbejdere_i_nexus",
                            group="Fejl",
                            json={
                                "CPR": opgave["cpr"],
                                "Fejl": f"Kunne ikke redigere opgave med navn: {nexus_opgave['title'] if nexus_opgave and 'title' in nexus_opgave else 'Ukendt'} i Nexus",
                            }
                    )
            blanket_data = {
                "formValues": [
                    {
                        "elementIdentifier": "RPASignatur",
                        "valueIdentifier": "Tekst",
                        "value": "Behandlet af Tyra (RPA)"      
                    },
                    {
                        "elementIdentifier": "RPABehandletDato",
                        "valueIdentifier": "Dato",
                        "value": datetime.today().strftime('%d-%m-%Y')      
                    }
                ]        
            }

            xflow_process_client.update_process(data["xflow_process_id"], blanket_data)
            xflow_process_client.advance_process(data["xflow_process_id"])

if __name__ == "__main__":    
    logging.basicConfig(
        level=logging.INFO        
    )

    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    nexus_credential = Credential.get_credential("KMD Nexus - produktion")
    nexus_database_credential = Credential.get_credential("KMD Nexus - database")
    xflow_credential = Credential.get_credential("Xflow - produktion")
    tracking_credential = Credential.get_credential("Odense SQL Server")
        
    nexus = NexusClientManager(
        client_id=nexus_credential.username,
        client_secret=nexus_credential.password,
        instance=nexus_credential.data["instance"],
    )    
    
    nexus_database_client = NexusDatabaseClient(
        host = nexus_database_credential.data["hostname"],
        port = nexus_database_credential.data["port"],
        user = nexus_database_credential.username,
        password = nexus_database_credential.password,
        database = nexus_database_credential.data["database_name"],
    )

    xflow_client = XFlowClient(
        token=xflow_credential.password,
        instance=xflow_credential.data["instance"],
    )
    xflow_process_client = ProcessClient(xflow_client)
    
    tracker = Tracker(
        username=tracking_credential.username, 
        password=tracking_credential.password
    )

    logger = logging.getLogger(__name__)

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue("new")
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
