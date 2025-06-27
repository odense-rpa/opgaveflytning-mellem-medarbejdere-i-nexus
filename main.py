import asyncio
import logging
import sys

from datetime import datetime
from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential
from nexus_database_client import NexusDatabaseClient
from kmd_nexus_client import NexusClient, CitizensClient, AssignmentsClient, OrganizationsClient
from xflow_client import XFlowClient, ProcessClient
from odk_tools.tracking import Tracker
from odk_tools.reporting import Reporter

nexus_client = None
citizens_client = None
assignments_client = None
organizations_client = None

nexus_database_client = None

xflow_client = None
xflow_process_client = None

tracker = None
reporter = None

proces_navn = "Opgaveflytning mellem medarbejdere i Nexus"

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
        
        flyt_opgaver_fra_initialer = xflow_process_client.find_process_element_value(proces, "FraMedarbejder", "Tekst")
        flyt_opgaver_til_initialer = xflow_process_client.find_process_element_value(proces, "TilMedarbejder", "Tekst")

        medarbejder_fra = organizations_client.get_professional_by_initials(flyt_opgaver_fra_initialer)

        if medarbejder_fra is None: 
            # Finder første (og eneste) acitivity tilhøerende RPAIntegration
            activity_id = next(activity["activitiesToReject"][0] for activity in proces["activities"] if activity["activityName"] == "RPAIntegration")
            xflow_process_client.reject_process(proces["publicId"], activity_id, f"Medarbejder med initialer: {medarbejder_fra} blev ikke fundet i Nexus")
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

            til_medarbedjer = None

            if data["to_initials"] is not None:
                medarbedjer = organizations_client.get_professional_by_initials(data["to_initials"])
                til_medarbedjer = {
                    "professionalId": medarbedjer["id"],
                    "displayName": medarbedjer["fullName"],
                    "displayNameWithUniqId": f"{medarbedjer['fullName']} ({medarbedjer['primaryIdentifier']})",
                    "active": medarbedjer["active"]
                }

            for opgave in opgaver:
                try:
                    citizen = citizens_client.get_citizen(opgave["cpr"])                    
                    nexus_opgave = assignments_client.get_assignment_by_citizen(citizen, opgave["id"])

                    if nexus_opgave is None:
                        reporter.report(
                            process=proces_navn,
                            group="Fejl",
                            json={
                                "CPR": opgave["cpr"],
                                "Fejl": "Kunne ikke finde opgave i Nexus",
                            }
                        )

                    nexus_opgave["professionalAssignee"] = til_medarbedjer

                    assignments_client.edit_assignment(nexus_opgave)
                    
                    tracker.track_task(proces_navn)
                except WorkItemError as e:
                    reporter.report(
                        process=proces_navn,
                        group="Fejl",
                        json={
                            "CPR": opgave["cpr"],
                            "Fejl": f"Kunne ikke redigere opgave med navn: {nexus_opgave["title"]} i Nexus",
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
    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    nexus_credential = Credential.get_credential("KMD Nexus - produktion")
    nexus_database_credential = Credential.get_credential("KMD Nexus - database")
    xflow_credential = Credential.get_credential("Xflow - produktion")
    tracking_credential = Credential.get_credential("Odense SQL Server")
    reporting_credential = Credential.get_credential("RoboA")
    
    nexus_client = NexusClient(
        client_id=nexus_credential.username,
        client_secret=nexus_credential.password,
        instance=nexus_credential.data["instance"],
    )
    citizens_client = CitizensClient(nexus_client=nexus_client)
    assignments_client = AssignmentsClient(nexus_client=nexus_client)
    organizations_client = OrganizationsClient(nexus_client=nexus_client)
    
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

    reporter = Reporter(
        username=reporting_credential.username,
        password=reporting_credential.password
    )

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue("new")
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
