import asyncio
import argparse
import logging
import os

from pathlib import Path
from datetime import datetime
from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential, WorkItemStatus
from kmd_nexus_client import NexusClientManager
from kmd_nexus_client.utils import sanitize_cpr
from kmd_nexus_client.tree_helpers import (
    filter_by_path
)
from odk_tools.tracking import Tracker
from odk_tools.reporting import report


proces_navn = "Journalisering af billånsafbetaling"

async def populate_queue(workqueue: Workqueue, directory: str, nexus: NexusClientManager):
    logger = logging.getLogger(__name__)
    directory_path = Path(directory)
    
    for pdf_file in directory_path.rglob("*.pdf"):
        file_path = str(pdf_file)
        filename = pdf_file.name
        
        if len(filename) >= 15:            
            cpr = filename[-15:][:11]           
            
            try:                
                cpr = sanitize_cpr(cpr)

            except Exception as e:
                logger.warning(f"Could not validate CPR {cpr} from file {filename}: {e}")
                continue
                       
                        
            # Add item to queue
            data = {
                "cpr": cpr,
                "file_path": file_path,
            }
            workqueue.add_item(data, cpr)
            logger.info(f"Tilføjede element til køen: CPR {cpr}, fil {file_path}")    


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)


    for item in workqueue:
        with item:
            data = item.data  # Item data deserialized from json as dict
 
            try:
                borger = nexus.borgere.hent_borger(data["cpr"])

                if borger is None:
                    raise WorkItemError(f"Borger med CPR {data['cpr']} ikke fundet i Nexus")                    

                nexus.forløb.opret_forløb(
                    borger=borger,
                    grundforløb_navn="Sundhedsfagligt grundforløb",
                    forløb_navn="Korrespondance  - Bilsag" # Dårlig naming med double mellemrum
                )

                visning = nexus.borgere.hent_visning(borger)
                referencer = nexus.borgere.hent_referencer(visning)

                forløb = filter_by_path(
                    referencer,
                    "/Sundhedsfagligt grundforløb/Korrespondance  - Bilsag",
                    active_pathways_only=True,
                )

                forløb = nexus.hent_fra_reference(forløb[0]) if len(forløb) > 0 else None

                if forløb is None:
                    raise WorkItemError(f"Forløb kunne ikke hentes efter oprettelse for CPR {data['cpr']}")
                
                nexus.forløb.opret_dokument(
                    borger=borger,
                    forløb=forløb,
                    fil=open(data["file_path"], "rb").read(),
                    filnavn=f"{data['cpr']}_Billaan_restancekort.pdf",
                    titel=f"{data['cpr']} - Restancekort - Billån",
                    noter="",
                    modtaget=datetime.now(),
                    indholdstype="application/pdf"
                )

                report(
                    report_id="journalisering-af-billaansafbetaling",
                    group="Gennemførte",
                    json={
                        "Cpr": data["cpr"],
                    }
                )
                
            except Exception as e:
                report(
                    report_id="journalisering-af-billaansafbetaling",
                    group="Manuelle",
                    json={
                        "Cpr": data["cpr"],
                        "Manuel årsag": str(e),
                    }
                )
                
                logger.error(f"Fejl ved behandling af element: {data}. Fejl: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    nexus_credential = Credential.get_credential("KMD Nexus - produktion")
    tracking_credential = Credential.get_credential("Odense SQL Server")

    tracker = Tracker(
        username=tracking_credential.username, password=tracking_credential.password
    )

    nexus = NexusClientManager(
        client_id=nexus_credential.username,
        client_secret=nexus_credential.password,
        instance=nexus_credential.data["instance"],
    )

    tracker = Tracker(
        username=tracking_credential.username, password=tracking_credential.password
    )

    # Parse command line arguments
    parser = argparse.ArgumentParser(description=proces_navn)
    parser.add_argument(
        "--directory",
        help="Path to the directory containing files to process",
        default="/mnt/rpa/Leverancer/Journalisering af billånsafbetaling"
    )
    parser.add_argument(
        "--queue",
        action="store_true",
        help="Populate the queue with test data and exit",
    )
    args = parser.parse_args()

    # Validate directory exists
    if not os.path.isdir(args.directory):
        raise FileNotFoundError(f"Directory not found: {args.directory}")
    
    # Queue management
    if args.queue:
        workqueue.clear_workqueue(WorkItemStatus.NEW)
        asyncio.run(populate_queue(workqueue, args.directory, nexus))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
