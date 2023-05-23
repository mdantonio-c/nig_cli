import json
import re
import tempfile
import time
import urllib.request
from contextlib import contextmanager
from datetime import datetime
from mimetypes import MimeTypes
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import dateutil.parser
import OpenSSL.crypto
import pytz
import requests
import typer

app = typer.Typer()

GET = "get"
POST = "post"
PUT = "put"
PATCH = "patch"

GB = 1_073_741_824
MB = 1_048_576
KB = 1024


class RequestMethodError(Exception):
    """Exception for unknown request method"""


class PhenotypeMalformedException(Exception):
    """Exception for malformed pedigree files"""


class HPOException(Exception):
    """Exception for invalid HPO"""


class ParsingSexException(Exception):
    """Exception for invalid sex"""


class AgeException(Exception):
    """Exception for invalid age"""


class PhenotypeNameException(Exception):
    """Exception for phenotypes that have names not related to an existing dataset"""


class RelationshipException(Exception):
    """Exception for a relationship between non existing phenotypes or generic errors in creating a relationship"""

    def __init__(self, error_message, r=None):
        self.message = error_message
        if r is not None:
            self.message += f". Code: {r.status_code}, response: {get_response(r)}"
        super().__init__(self.message)


class GeodataException(Exception):
    """Exception for errors in geodata"""


class TechnicalMalformedException(Exception):
    """Exception for malformed technical files"""


class UnknownPlatformException(Exception):
    """Exception for unknown platform for technicals"""


class TechnicalAssociationException(Exception):
    """Exception for technicals with a non existing dataset associated"""


class ResourceCreationException(Exception):
    """Exception for errors in creating resources"""

    def __init__(self, error_message, r=None):
        self.message = error_message
        if r is not None:
            self.message += f". Code: {r.status_code}, response: {get_response(r)}"
        super().__init__(self.message)


class ResourceRetrievingException(Exception):
    """Exception for errors in retrieving resources"""

    def __init__(self, error_message, r=None):
        self.message = error_message
        if r is not None:
            self.message += f". Code: {r.status_code}, response: {get_response(r)}"
        super().__init__(self.message)


class ResourceAssignationException(Exception):
    """Exception for errors in assignating resources to other resources (ex. phenotypes to a dataset)"""

    def __init__(self, error_message, r=None):
        self.message = error_message
        if r is not None:
            self.message += f". Code: {r.status_code}, response: {get_response(r)}"
        super().__init__(self.message)


class ResourceModificationException(Exception):
    """Exception for errors in modify resources"""

    def __init__(self, error_message, r=None):
        self.message = error_message
        if r is not None:
            self.message += f". Code: {r.status_code}, response: {get_response(r)}"
        super().__init__(self.message)


class UploadInitException(Exception):
    """Exception for errors in initializing an upload"""

    def __init__(self, error_message, r=None):
        self.message = error_message
        if r is not None:
            self.message += f". Code: {r.status_code}, response: {get_response(r)}"
        super().__init__(self.message)


class UploadException(Exception):
    """Exception for errors in uploading a file"""

    def __init__(self, error_message, r=None):
        self.message = error_message
        if r is not None:
            self.message += f". Code: {r.status_code}, response: {get_response(r)}"
        super().__init__(self.message)

class RetrieveExistingStudyException(Exception):
    """Exception for not find the already esxisting study """


@contextmanager
def pfx_to_pem(pfx_path: Path, pfx_password: str) -> Generator[str, None, None]:
    """Decrypts the .pfx file to be used with requests."""
    with tempfile.NamedTemporaryFile(suffix=".pem") as t_pem:
        f_pem = open(t_pem.name, "wb")
        pfx = open(pfx_path, "rb").read()
        p12 = OpenSSL.crypto.load_pkcs12(pfx, pfx_password.encode())
        f_pem.write(
            OpenSSL.crypto.dump_privatekey(
                OpenSSL.crypto.FILETYPE_PEM, p12.get_privatekey()
            )
        )
        f_pem.write(
            OpenSSL.crypto.dump_certificate(
                OpenSSL.crypto.FILETYPE_PEM, p12.get_certificate()
            )
        )
        ca = p12.get_ca_certificates()
        if ca is not None:
            for cert in ca:
                f_pem.write(
                    OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, cert)
                )
        f_pem.close()
        yield t_pem.name


def request(
    method: str,
    url: str,
    certfile: Path,
    certpwd: str,
    data: Union[bytes, Dict[str, Any]],
    headers: Optional[Dict[str, Any]] = None,
) -> requests.Response:
    MAX_RETRIES = 3
    SLEEP_TIME = 10

    with pfx_to_pem(certfile, certpwd) as cert:
        if method == POST:
            for i in range(MAX_RETRIES):
                try:
                    r = requests.post(
                        url,
                        data=data,
                        headers=headers,
                        timeout=15,
                        cert=cert,
                    )
                    return r
                except Exception as e:
                    error(f"The request raised the following error {e}")
                    if i < MAX_RETRIES:
                        debug(f"Retry n.{i + 1} will be done in {SLEEP_TIME} seconds")
                    time.sleep(SLEEP_TIME)
                    continue

        if method == PUT:
            for i in range(MAX_RETRIES):
                try:
                    r = requests.put(
                        url,
                        data=data,
                        headers=headers,
                        timeout=15,
                        cert=cert,
                    )
                    return r
                except Exception as e:
                    error(f"The request raised the following error {e}")
                    if i < MAX_RETRIES:
                        debug(f"Retry n.{i + 1} will be done in {SLEEP_TIME} seconds")
                    time.sleep(SLEEP_TIME)
                    continue

        if method == PATCH:
            for i in range(MAX_RETRIES):
                try:
                    r = requests.patch(
                        url,
                        data=data,
                        headers=headers,
                        timeout=15,
                        cert=cert,
                    )
                    return r
                except Exception as e:
                    error(f"The request raised the following error {e}")
                    if i < MAX_RETRIES:
                        debug(f"Retry n.{i + 1} will be done in {SLEEP_TIME} seconds")
                    time.sleep(SLEEP_TIME)
                    continue

        if method == GET:
            for i in range(MAX_RETRIES):
                try:
                    r = requests.get(
                        url,
                        headers=headers,
                        timeout=15,
                        cert=cert,
                    )
                    return r
                except Exception as e:
                    error(f"The request raised the following error {e}")
                    if i < MAX_RETRIES:
                        debug(f"Retry n.{i + 1} will be done in {SLEEP_TIME} seconds")
                    time.sleep(SLEEP_TIME)
                    continue

        # if hasn't returned yet is because the method is unknown
        raise RequestMethodError(f"method {method} not allowed")


def error(text: str, r: Optional[requests.Response] = None) -> None:
    if r is not None:
        text += f". Status: {r.status_code}, response: {get_response(r)}"
    typer.secho(text, fg=typer.colors.RED)
    return None


def warning(text: str) -> None:
    typer.secho(text, fg=typer.colors.YELLOW)
    return None


def success(text: str) -> None:
    typer.secho(text, fg=typer.colors.GREEN)
    return None


def debug(text: str) -> None:
    typer.secho(text, fg=typer.colors.BLUE)
    return None


def get_response(r: requests.Response) -> Any:
    if r.text:
        return r.text
    return r.json()


def get_value(key: str, header: List[str], line: List[str]) -> Optional[str]:
    if not header:
        return None
    if key not in header:
        return None
    index = header.index(key)
    if index >= len(line):
        return None
    value = line[index]
    if not value:
        return None
    if value == "-":
        return None
    if value == "N/A":
        return None
    return value


def date_from_string(date: str, fmt: str = "%d/%m/%Y") -> Optional[datetime]:

    if date == "":
        return None
    # datetime.now(pytz.utc)
    try:
        return_date = datetime.strptime(date, fmt)
    except BaseException:
        return_date = dateutil.parser.parse(date)

    # TODO: test me with: 2017-09-22T07:10:35.822772835Z
    if return_date.tzinfo is None:
        return pytz.utc.localize(return_date)

    return return_date


def parse_file_ped(
    file: Path, datasets: Dict[str, List[Path]]
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, List[str]]]]:
    with open(file) as f:

        header: List[str] = []
        phenotype_list: List[str] = []
        phenotypes: List[Dict[str, Any]] = []
        relationships: Optional[Dict[str, List[str]]] = {}
        while True:
            row = f.readline()
            if not row:
                break

            if row.startswith("#"):
                # Remove the initial #
                row = row[1:].strip().lower()
                # header = re.split(r"\s+|\t", line)
                header = re.split(r"\t", row)
                continue

            row = row.strip()
            # line = re.split(r"\s+|\t", line)
            line = re.split(r"\t", row)

            if len(line) < 5:
                raise PhenotypeMalformedException(
                    "Error parsing the peedigree file: not all the mandatory fields are present"
                )

            # pedigree_id = line[0]
            individual_id = line[1]
            # validate phenotypes: check if they are associated to an existing dataset
            if individual_id not in datasets.keys():
                # phenotype has to have the same name of the dataset to be associated
                raise PhenotypeNameException(
                    f"Phenotype {individual_id} is not related to any existing dataset"
                )
            father = line[2]
            mother = line[3]
            sex = line[4]

            if sex == "1" or sex == "M":
                sex = "male"
            elif sex == "2" or sex == "F":
                sex = "female"
            else:
                raise ParsingSexException(
                    f"Can't parse {sex} sex for {individual_id}: Please use M F notation"
                )

            properties = {}
            properties["name"] = individual_id
            properties["sex"] = sex

            age = get_value("age", header, line)
            if age is not None:
                if int(age) < 0:
                    raise AgeException(
                        f"Phenotype {individual_id}: {age} is not a valid age"
                    )
                properties["age"] = int(age)

            birth_place = get_value("birthplace", header, line)
            if birth_place is not None and birth_place != "-":
                properties["birth_place_name"] = birth_place

            hpo = get_value("hpo", header, line)
            if hpo is not None:
                hpo_list = hpo.split(",")
                for hpo_el in hpo_list:
                    if not re.match(r"HP:[0-9]+$", hpo_el):
                        raise HPOException(
                            f"Error parsing phenotype {individual_id}: {hpo_el} is an invalid HPO"
                        )
                properties["hpo"] = json.dumps(hpo_list)

            phenotypes.append(properties)
            phenotype_list.append(individual_id)

            # parse relationships
            relationships[individual_id] = []

            if father and father != "-":
                relationships[individual_id].append(father)

            if mother and mother != "-":
                relationships[individual_id].append(mother)

            # if the phenotype has not relationships, delete the key
            if not relationships[individual_id]:
                del relationships[individual_id]

    # check if relationships are valid
    if relationships:
        for son, family in relationships.items():
            for parent in family:
                if parent not in phenotype_list:
                    raise RelationshipException(
                        f"Error in relationship between {son} and {parent}: Phenotype {parent} does not exist"
                    )

    return phenotypes, relationships


def parse_file_tech(
    file: Path, datasets: Dict[str, List[Path]]
) -> List[Dict[str, Any]]:

    supported_platforms = [
        "Illumina",
        "Ion",
        "Pacific Biosciences",
        "Roche 454",
        "SOLiD",
        "SNP-array",
        "Other",
    ]

    with open(file) as f:

        header: List[str] = []
        technicals: List[Dict[str, Any]] = []
        while True:
            row = f.readline()
            if not row:
                break

            if row.startswith("#"):
                # Remove the initial #
                row = row[1:].strip().lower()
                # header = re.split(r"\s+|\t", row)
                header = re.split(r"\t", row)
                continue

            row = row.strip()
            # line = re.split(r"\s+|\t", row)
            line = re.split(r"\t", row)

            if len(line) < 4:
                raise TechnicalMalformedException(
                    "Error parsing the technical metadata file: not all the mandatory fields are present"
                )

            name = line[0]
            date = line[1]
            platform = line[2]
            kit = line[3]

            technical = {}
            properties = {}
            properties["name"] = name
            if date and date != "-":
                properties["sequencing_date"] = date_from_string(date).date()
            else:
                properties["sequencing_date"] = ""

            if platform and platform not in supported_platforms:
                raise UnknownPlatformException(
                    f"Error for {name} technical: Platform has to be one of {supported_platforms}"
                )
            properties["platform"] = platform
            properties["enrichment_kit"] = kit
            technical["properties"] = properties

            value = get_value("dataset", header, line)
            if value is not None and value != "-":
                dataset_list = value.split(",")
                for dataset_name in dataset_list:
                    if dataset_name not in datasets.keys():
                        raise TechnicalAssociationException(
                            f"Error for {name} technical: associated dataset {dataset_name} does not exist"
                        )
                technical["datasets"] = dataset_list
            technicals.append(technical)
    # check dataset association for technicals
    if len(technicals) > 1:
        associated_datasets = []
        for tech in technicals:
            if "datasets" not in tech.keys():
                raise TechnicalAssociationException(
                    f"Technical {tech['properties']['name']} is not associated to any dataset"
                )
            for d in tech["datasets"]:
                if d in associated_datasets:
                    raise TechnicalAssociationException(
                        f"Dataset {d} has multiple technicals associated"
                    )
                associated_datasets.append(d)

    return technicals


def version_callback(value: bool) -> None:
    if value:
        typer.echo("NIG Upload version: 0.4.1")
        raise typer.Exit()


def pluralize(value: int, unit: str) -> str:
    if value == 1:
        return f"{value} {unit}"
    return f"{value} {unit}s"


# from restapi.utilities.time
def get_time(seconds: int) -> str:

    elements: List[str] = []
    if seconds < 60:
        elements.append(pluralize(seconds, "second"))

    elif seconds < 3600:
        m, s = divmod(seconds, 60)
        elements.append(pluralize(m, "minute"))
        if s > 0:
            elements.append(pluralize(s, "second"))

    elif seconds < 86400:
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        elements.append(pluralize(h, "hour"))
        if m > 0 or s > 0:
            elements.append(pluralize(m, "minute"))
        if s > 0:
            elements.append(pluralize(s, "second"))
    else:
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        d, h = divmod(h, 24)
        elements.append(pluralize(d, "day"))
        if h > 0 or m > 0 or s > 0:
            elements.append(pluralize(h, "hour"))
        if m > 0 or s > 0:
            elements.append(pluralize(m, "minute"))
        if s > 0:
            elements.append(pluralize(s, "second"))

    return ", ".join(elements)


# from controller.utilities.system
def get_speed(value: float) -> str:

    if value >= GB:
        value /= GB
        unit = " GB/s"
    elif value >= MB:
        value /= MB
        unit = " MB/s"
    elif value >= KB:
        value /= KB
        unit = " KB/s"
    else:
        unit = " B/s"

    return f"{round(value, 2)}{unit}"


def get_ip() -> str:
    return urllib.request.urlopen("https://ident.me").read().decode("utf8")


def validate_study(study: Path) -> Optional[Dict[str, Any]]:

    study_tree: Dict[str, Any] = {
        "name": study.name,
        "phenotypes": "",
        "technicals": "",
        "datasets": {},
    }

    for d in study.iterdir():
        if d.is_dir():
            for dat in d.iterdir():
                if (
                    dat.is_file()
                    and dat.name.endswith(".fastq.gz")
                    and dat.stat().st_size >= 1
                ):
                    study_tree["datasets"].setdefault(d.name, [])
                    study_tree["datasets"][d.name].append(dat)
                else:
                    warning(f"File {dat} skipped")
                    debug(
                        f"DEBUG : skipped because is not a file? { not dat.is_file()}, skipped because is empty? {dat.stat().st_size < 1}, has the correct file extension (.fastq.gz)? {dat.name.endswith('.fastq.gz')}"
                    )
            if (
                study_tree["datasets"].get(d.name)
                and len(study_tree["datasets"][d.name]) > 2
            ):
                # the dataset is invalid because contains too many fastq
                warning(
                    f"Upload of {study.name} skipped: Dataset {d.name} contains too many fastq files: max allowed files are 2 per dataset"
                )
                return None
        else:
            if d.name != "technical.txt" and d.name != "pedigree.txt":
                warning(f"{d} is not a directory")

    if not study_tree["datasets"]:
        warning(
            f"Upload of {study.name} skipped: No files found for upload in: {study}"
        )
        return None

    pedigree = study.joinpath("pedigree.txt")
    if pedigree.is_file():
        try:
            phenotypes_list, relationships = parse_file_ped(
                pedigree, study_tree["datasets"]
            )
        except (
            PhenotypeMalformedException,
            PhenotypeNameException,
            HPOException,
            ParsingSexException,
            AgeException,
            RelationshipException,
        ) as exc:
            warning(f"Upload of {study.name} skipped: {exc}")
            return None

        study_tree["phenotypes"] = phenotypes_list
        study_tree["relationships"] = relationships

    technical = study.joinpath("technical.txt")
    if technical.is_file():
        try:
            technicals_list = parse_file_tech(technical, study_tree["datasets"])
        except (
            TechnicalMalformedException,
            UnknownPlatformException,
            TechnicalAssociationException,
        ) as exc:
            warning(f"Upload of {study.name} skipped: {exc}")
            return None

        study_tree["technicals"] = technicals_list

    return study_tree

def update_study_tree(study: Path, study_tree: Dict[str, Any] ,url: str, headers: Dict[str, str] , certfile: Path, certpwd: str) -> Optional[Dict[str, Any]]:
    updated_study_tree: Optional[Dict[str, Any]] = {}
    # get the study uuid
    study_uuid: Optional[str]= None
    r = request(
        method=GET,
        url=f"{url}api/study",
        headers=headers,
        certfile=certfile,
        certpwd=certpwd,
        data={},
    )
    if r.status_code != 200:
        raise ResourceRetrievingException("Can't retrieve user's studies list", r)

    res = r.json()
    if res:
        for el in res:
            if el["name"] == study_tree["name"]:
                study_uuid = el["uuid"]
                break
    if not study_uuid:
        raise RetrieveExistingStudyException(f" Study {study_tree['name']} is supposed to be already existing but it can't be found")

    updated_study_tree["study_uuid"] = study_uuid
    updated_study_tree["name"] = study_tree["name"]
    # get the already existing datasets
    r = request(
        method=GET,
        url=f"{url}api/study/{study_uuid}/datasets",
        headers=headers,
        certfile=certfile,
        certpwd=certpwd,
        data={},
    )
    if r.status_code != 200:
        raise ResourceRetrievingException(
            "Can't retrieve user's datasets list", r
        )

    res = r.json()
    datasets_map: Optional[Dict[str,Dict[str,str]]] = {}
    if res:
        for el in res:
            dataset_descr = {"uuid":el["uuid"], "status": el["status"]}
            datasets_map[el["name"]] = dataset_descr
    if datasets_map:
        updated_study_tree['datasets']={}
        # check what datasets has already to be uploaded
        for d in study_tree['datasets'].keys():
            if d in datasets_map.keys():
                if not datasets_map[d]["status"]:
                    warning(f"WARNING: Dataset {d} is not checked as ready to be analyzed")
                    # get the files list
                    r = request(
                        method=GET,
                        url=f"{url}api/dataset/{datasets_map[d]['uuid']}/files",
                        headers=headers,
                        certfile=certfile,
                        certpwd=certpwd,
                        data={},
                    )
                    if r.status_code != 200:
                        raise ResourceRetrievingException(
                            "Can't retrieve dataset' files list", r
                        )

                    res = r.json()
                    files_map: Optional[Dict[str, Dict[str, str]]] = {}
                    if res:
                        for el in res:
                            file_el= {"status":el["status"]}
                            files_map[el["name"]] = file_el

                    # check if the files has been correctly uploaded
                    for f in study_tree['datasets'][d]:
                        if f.name not in files_map.keys() or files_map[f.name]["status"] != "uploaded":
                            error(f"File {f.name} in Dataset {d} wasn't correctly uploaded: Please check")

                # remove the dataset from the list of dataset to upload
                warning(f"Dataset {d} already existing: it will not be updated")
            else:
                updated_study_tree['datasets'][d] = study_tree['datasets'][d]

    if 'datasets' not in updated_study_tree.keys():
        warning(
            f"Update of Study {study_tree['name']} skipped: No new datasets to add has been found"
        )
        return None

    # check the phenotypes
    phenotypes_to_upload: Optional[List[str]] = []
    if study_tree["phenotypes"]:
        # get the study phenotypes
        r = request(
            method=GET,
            url=f"{url}api/study/{study_uuid}/phenotypes",
            headers=headers,
            certfile=certfile,
            certpwd=certpwd,
            data={},
        )
        if r.status_code != 200:
            raise ResourceRetrievingException("Can't retrieve user's studies list", r)

        res = r.json()
        existing_phenotypes: Dict[str,str] = {}
        if res:
            for el in res:
                existing_phenotypes[el["name"]] = el["uuid"]

        updated_study_tree['phenotypes']=[]
        for p in study_tree['phenotypes']:
            if p["name"] in existing_phenotypes.keys():
                # don't add the phenotypes to the list of phenotypes to upload
                warning(f"Phenotype {p['name']} already existing: it will not be updated")
                # mark the phenotype as already existing: this will be only used in case of relationships creation
                p["uuid"] = existing_phenotypes[p["name"]]
            else:
                if p["name"] not in updated_study_tree['datasets'].keys():
                    raise PhenotypeNameException(f"Phenotype {p['name']} has to be created but is not related to any dataset already to be uploaded: Please check")
                phenotypes_to_upload.append(p["name"])
            updated_study_tree['phenotypes'].append(p)


    # check that the relationships are related to not already existing phenotypes
    if "relationships" in study_tree.keys():
        if not phenotypes_to_upload:
            warning(f"No new phenotypes to add: relationships between already existing phenotypes will not be updated")
            study_tree.pop("relationships")
        else:
            updated_study_tree["relationships"]={}
            for r in study_tree["relationships"].keys():
                if r not in phenotypes_to_upload:
                    warning(f"Relationship related to already existing Phenotype{r} will not be updated")
                else:
                    updated_study_tree["relationships"][r]=study_tree["relationships"][r]


    # check the technicals
    if study_tree["technicals"]:
        # get the study technicals
        r = request(
            method=GET,
            url=f"{url}api/study/{study_uuid}/technicals",
            headers=headers,
            certfile=certfile,
            certpwd=certpwd,
            data={},
        )
        if r.status_code != 200:
            raise ResourceRetrievingException("Can't retrieve user's studies list", r)

        res = r.json()
        existing_technicals: Dict[str,str] = {}
        if res:
            for el in res:
                existing_technicals[el["name"]] = el["uuid"]

        updated_study_tree["technicals"]=[]
        for t in study_tree["technicals"]:
            # check if it is related to a dataset to be uploaded
            if "datasets" in t.keys():
                technical_datasets: Optional[List[str]] = []
                for d in t["datasets"]:
                    if d in updated_study_tree["datasets"].keys():
                        technical_datasets.append(d)
                    else:
                        if t["properties"]["name"] not in existing_technicals.keys():
                            raise TechnicalAssociationException(f"Technical {t['properties']['name']} has to be created but is not related to any dataset already to be uploaded: Please check")
                # if now this list is empty the technical can be removed from the upload list
                if not technical_datasets:
                    continue
                else:
                    t["datasets"] = technical_datasets
                    updated_study_tree["technicals"].append(t)
            if t["properties"]["name"] in existing_technicals.keys():
                warning(f"Technical {t['properties']['name']} already existing: it will not be updated")
                # mark the technical as already existing: this will be only associated to the new datasets
                t["uuid"] = existing_technicals[t["properties"]["name"]]
            # add the technical to the updated tree
            updated_study_tree["technicals"].append(t)

    

    return updated_study_tree


def get_technical_uuid(
    study_tree: Dict[str, Any], dataset_name: str, technicals_uuid: Dict[str, str]
) -> Optional[str]:
    tech_uuid: Optional[str] = None
    if len(study_tree["technicals"]) > 1:
        for tech in study_tree["technicals"]:
            if dataset_name in tech["datasets"]:
                tech_uuid = technicals_uuid[tech["properties"]["name"]]
                break
    else:
        if (
            "datasets" not in study_tree["technicals"][0].keys()
            or "datasets" in study_tree["technicals"][0].keys()
            and dataset_name in study_tree["technicals"][0]["datasets"]
        ):
            tech_uuid = technicals_uuid[
                study_tree["technicals"][0]["properties"]["name"]
            ]
    return tech_uuid


def upload_study(
    study_tree: Dict[str, Any],
    url: str,
    headers: Dict[str, str],
    certfile: Path,
    certpwd: str,
    chunk_size: int,
    IP_ADDR: str,
) -> None:
    # if study_uuid is in the study_tree it means that we are in the already existing study use case and there is no need to create the study
    if "study_uuid" not in study_tree.keys():
        study_name = study_tree["name"]
        r = request(
            method=POST,
            url=f"{url}api/study",
            headers=headers,
            certfile=certfile,
            certpwd=certpwd,
            data={"name": study_name, "description": ""},
        )
        if r.status_code != 200:
            raise ResourceCreationException("Study creation failed", r)

        success(f"Succesfully created study {study_name}")

        study_uuid = r.json()
    else:
        study_uuid = study_tree["study_uuid"]

    # create phenotypes
    phenotypes_uuid: Dict[str, str] = {}
    if study_tree["phenotypes"]:
        are_phenotypes_to_upload = False
        # manage existing study use case: check if there are phenotype to upload
        for p in study_tree["phenotypes"]:
            if "uuid" in p.keys():
                phenotypes_uuid[p["name"]] = p["uuid"]
            else:
                # there is at least one phenotype to upload
                are_phenotypes_to_upload = True

        if are_phenotypes_to_upload:
            # get geodata list
            headers["Content-Type"] = "application/json"
            r = request(
                method=POST,
                url=f"{url}api/study/{study_uuid}/phenotypes",
                headers=headers,
                certfile=certfile,
                certpwd=certpwd,
                data='{"get_schema": true}',
            )
            if r.status_code != 200:
                raise ResourceRetrievingException("Can't retrieve geodata list", r)

            for el in r.json():
                if el["key"] == "birth_place":
                    geodata = el["options"]
                    break
            for phenotype in study_tree["phenotypes"]:
                # check if we are in the already existing study use case and the phenotype already exists
                if "uuid" not in phenotype.keys():
                    # get the birth_place
                    if phenotype.get("birth_place_name"):
                        for geo_id, name in geodata.items():
                            if name == phenotype["birth_place_name"]:
                                phenotype["birth_place"] = geo_id
                                break
                        if "birth_place" not in phenotype.keys():
                            raise GeodataException(
                                f"Error for phenotype {phenotype['name']}: {phenotype['birth_place_name']} birth place not found"
                            )

                        # delete birth_place_name key
                        del phenotype["birth_place_name"]

                    headers.pop("Content-Type", None)
                    r = request(
                        method=POST,
                        url=f"{url}api/study/{study_uuid}/phenotypes",
                        headers=headers,
                        certfile=certfile,
                        certpwd=certpwd,
                        data=phenotype,
                    )
                    if r.status_code != 200:
                        raise ResourceCreationException("Phenotype creation failed", r)

                    success(f"Succesfully created phenotype {phenotype['name']}")

                    # add the uuid in the phenotype uuid dictionary
                    phenotypes_uuid[phenotype["name"]] = r.json()


    # create phenotypes relationships
    if "relationships" in study_tree.keys():
        for son, parent_list in study_tree["relationships"].items():
            son_uuid = phenotypes_uuid.get(son)
            for parent in parent_list:
                parent_uuid = phenotypes_uuid.get(parent)
                r = request(
                    method=POST,
                    url=f"{url}api/phenotype/{son_uuid}/relationships/{parent_uuid}",
                    headers=headers,
                    certfile=certfile,
                    certpwd=certpwd,
                    data={},
                )
                if r.status_code != 200:
                    raise RelationshipException("Phenotype relationship failed", r)

                success(f"Succesfully created relationship between {son} and {parent}")

    # create technicals
    technicals_uuid: Dict[str, str] = {}
    if study_tree["technicals"]:
        for technical in study_tree["technicals"]:
            # check if we are in the already existing study use case and the technical already exists
            if "uuid" not in technical.keys():
                r = request(
                    method=POST,
                    url=f"{url}api/study/{study_uuid}/technicals",
                    headers=headers,
                    certfile=certfile,
                    certpwd=certpwd,
                    data=technical["properties"],
                )
                if r.status_code != 200:
                    raise ResourceCreationException("Technical creation failed", r)
    
                success(f"Succesfully created technical {technical['properties']['name']}")

                # add the uuid in the technical uuid dictionary
                technicals_uuid[technical["properties"]["name"]] = r.json()
            else:
                # add the uuid in the technical uuid dictionary
                technicals_uuid[technical["properties"]["name"]] = technical["uuid"]

    for dataset_name, files in study_tree["datasets"].items():
        r = request(
            method=POST,
            url=f"{url}api/study/{study_uuid}/datasets",
            headers=headers,
            certfile=certfile,
            certpwd=certpwd,
            data={"name": dataset_name, "description": ""},
        )

        if r.status_code != 200:
            raise ResourceCreationException("Dataset creation failed", r)

        success(f"Succesfully created dataset {dataset_name}")
        uuid = r.json()

        #  connect the phenotype to the dataset
        if dataset_name in phenotypes_uuid.keys():
            phen_uuid = phenotypes_uuid[dataset_name]
            r = request(
                method=PUT,
                url=f"{url}api/dataset/{uuid}",
                headers=headers,
                certfile=certfile,
                certpwd=certpwd,
                data={"phenotype": phen_uuid},
            )
            if r.status_code != 204:
                raise ResourceAssignationException(
                    "Can't assign a phenotype to the dataset", r
                )

            success(f"Succesfully assigned phenotype to dataset {dataset_name}")

        #  connect the technical to the dataset
        if study_tree["technicals"]:
            tech_uuid = get_technical_uuid(study_tree, dataset_name, technicals_uuid)

            if tech_uuid:
                r = request(
                    method=PUT,
                    url=f"{url}api/dataset/{uuid}",
                    headers=headers,
                    certfile=certfile,
                    certpwd=certpwd,
                    data={"technical": tech_uuid},
                )
                if r.status_code != 204:
                    raise ResourceAssignationException(
                        "Can't assign a technical to the dataset", r
                    )

                success(f"Succesfully assigned technical to dataset {dataset_name}")

        for file in files:
            # get the data for the upload request
            filename = file.name
            filesize = file.stat().st_size
            mimeType = MimeTypes().guess_type(str(file))
            lastModified = int(file.stat().st_mtime)

            data = {
                "name": filename,
                "mimeType": mimeType,
                "size": filesize,
                "lastModified": lastModified,
            }

            # init the upload
            r = request(
                method=POST,
                url=f"{url}api/dataset/{uuid}/files/upload",
                headers=headers,
                certfile=certfile,
                certpwd=certpwd,
                data=data,
            )

            if r.status_code != 201:
                raise UploadInitException("Can't start the upload", r)

            success("Upload succesfully initialized")

            chunk = chunk_size * 1024 * 1024
            range_start = -1
            prev_position = 0

            with open(file, "rb") as f:
                start = datetime.now()
                with typer.progressbar(length=filesize, label="Uploading") as progress:
                    while True:

                        prev_position = f.tell()
                        read_data = f.read(chunk)
                        # No more data read from the file
                        if not read_data:
                            break

                        range_start += 1

                        range_max = min(range_start + chunk, filesize)

                        content_range = f"bytes {range_start}-{range_max}/{filesize}"
                        headers["Content-Range"] = content_range

                        try:

                            r = request(
                                method=PUT,
                                url=f"{url}api/dataset/{uuid}/files/upload/{filename}",
                                headers=headers,
                                certfile=certfile,
                                certpwd=certpwd,
                                data=read_data,
                            )
                        except (
                            requests.exceptions.ConnectionError,
                            requests.exceptions.ReadTimeout,
                        ) as r:

                            IP = get_ip()
                            if IP != IP_ADDR:
                                return error(
                                    f"\nUpload failed due to a network error ({r})"
                                    f"\nYour IP address changed from {IP_ADDR} to {IP}."
                                    "\nDue to security policies the upload"
                                    " can't be retried"
                                )
                            else:
                                error(f"Upload Failed, retrying ({str(r)})")
                                f.seek(prev_position)
                                range_start -= 1
                                continue

                        if r.status_code != 206:
                            if r.status_code == 200:
                                # upload is complete
                                progress.update(filesize)
                                break
                            raise UploadException("Upload Failed", r)

                        progress.update(chunk)
                        # update the range variable
                        range_start += chunk

                end = datetime.now()
                seconds = (end - start).seconds or 1

                t = get_time(seconds)
                s = get_speed(filesize / seconds)
                if r.status_code != 200:
                    raise UploadException(f"Upload Failed in {t} ({s})", r)

                success(f"Upload succesfully completed in {t} ({s})")

        # set the status of the dataset as "UPLOAD COMPLETED"
        r = request(
            method=PATCH,
            url=f"{url}api/dataset/{uuid}",
            headers=headers,
            certfile=certfile,
            certpwd=certpwd,
            data={"status": "UPLOAD COMPLETED"},
        )
        if r.status_code != 204:
            raise ResourceModificationException(
                "Can't set the status to the dataset", r
            )

        success(f"Succesfully set UPLOAD COMPLETE to {dataset_name}")


@app.command()
def upload(
    study: Path = typer.Option(None, help="Path to the study"),
    studies: Path = typer.Option(
        None, help="Path to the main folder containing the studies directories"
    ),
    url: str = typer.Option(..., prompt="Server URL", help="Server URL"),
    username: str = typer.Option(..., prompt="Your username"),
    pwd: str = typer.Option(..., prompt="Your password", hide_input=True),
    certfile: Path = typer.Option(
        ..., prompt="Path of your certificate", help="Path of the certificate file"
    ),
    certpwd: str = typer.Option(
        ...,
        prompt="Password of your certificate",
        hide_input=True,
        help="Password of the certificate",
    ),
    totp: str = typer.Option(..., prompt="2FA TOTP"),
    chunk_size: int = typer.Option(16, "--chunk-size", help="Upload chunk size in MB"),
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Print version information and quit",
        show_default=False,
        callback=version_callback,
        is_eager=True,
    ),
) -> None:

    if not study and not studies:
        return error(
            "A path to a study or to a directory of studies has to be specified"
        )

    if not url.startswith("https:"):
        url = f"https://{url}"
    if not url.endswith("/"):
        url = f"{url}/"

    if not certfile.exists():
        return error(f"Certificate not found: {certfile}")

    if chunk_size > 16:
        return error(f"The specified chunk size is too large: {chunk_size}")

    try:
        IP_ADDR = get_ip()
        success(f"Your IP address is {IP_ADDR}")

        # Do login
        r = request(
            method=POST,
            url=f"{url}auth/login",
            certfile=certfile,
            certpwd=certpwd,
            data={"username": username, "password": pwd, "totp_code": totp},
        )

        if r.status_code != 200:
            if r.text:
                print(r.text)
                return error(f"Login Failed. Status: {r.status_code}")

            return error("Login Failed", r)

        token = r.json()
        headers = {"Authorization": f"Bearer {token}"}
        success("Succesfully logged in")

    except RequestMethodError as exc:
        return error(exc)

    # get a list of the path to the studies to upload
    studies_to_upload: List[Path] = []
    if study:
        # check if the input directory exists
        if not study.exists():
            return error(f"The specified study does not exists: {study}")
        studies_to_upload.append(study)
    else:
        # check if the input directory exists
        if not studies.exists():
            return error(
                f"The specified directory containing the studies directories does not exists: {studies}"
            )
        for d in studies.iterdir():
            if d.is_dir():
                studies_to_upload.append(d)
        if not studies_to_upload:
            return error(f"No studies found in {studies}")

    try:
        # get user studies list
        existing_studies: Dict[str, str] = {}
        r = request(
            method=GET,
            url=f"{url}api/study",
            headers=headers,
            certfile=certfile,
            certpwd=certpwd,
            data={},
        )
        if r.status_code != 200:
            raise ResourceRetrievingException("Can't retrieve user's studies list", r)

        res = r.json()
        if res:
            for el in res:
                existing_studies[el["name"]] = el["uuid"]

        for s in studies_to_upload:
            # check if the study already exists
            is_existing_study = False
            if s.name in existing_studies.keys():
                is_existing_study = True
                # get the list of the datasets in the study to upload
                datasets_to_upload: List[str] = []
                for d in s.iterdir():
                    if d.is_dir():
                        datasets_to_upload.append(d.name)
                # get the list of the datasets of the existing study
                existing_datasets: List[str] = []
                r = request(
                    method=GET,
                    url=f"{url}api/study/{existing_studies[s.name]}/datasets",
                    headers=headers,
                    certfile=certfile,
                    certpwd=certpwd,
                    data={},
                )
                if r.status_code != 200:
                    raise ResourceRetrievingException(
                        "Can't retrieve user's datasets list", r
                    )

                res = r.json()
                if res:
                    for el in res:
                        existing_datasets.append(el["name"])
                # if the two list differs ask if the user wants to continue
                if not set(datasets_to_upload) == set(existing_datasets):
                    warning(f"WARNING: Study {s.name} already exists but its datasets differ from the already uploaded")
                    continue_upload = typer.confirm("Continue the upload anyway? Note that the already existing datasets, technicals and samples of this study will not be updated")
                    if not continue_upload:
                        return error(
                            f"Upload of already existing Study {s.name} has been aborted by the user"
                        )
                else:
                    # the study has already been uploaded
                    warning(f"Study {s.name} already exists: skipped")
                    continue

            # validate study
            study_tree = validate_study(s)
            if not study_tree:
                # the study hasn't passed the validation
                continue
            # if the study already exists check which elements of the study tree needs to be uploaded
            if is_existing_study:
                study_tree = update_study_tree(s, study_tree, url, headers, certfile, certpwd)
                if not study_tree:
                    continue
                approve_differences = typer.confirm(
                    f"The following datasets are missing from the Study {s.name} and will be uploaded: {[d for d in study_tree['datasets'].keys()]}. "
                    f"The following phenotypes are missing from the Study {s.name} and will be uploaded: {[p['name'] for p in study_tree['phenotypes'] if 'uuid' not in p.keys()]}."
                    f"The following technicals are missing from the Study {s.name} and will be uploaded: {[t['properties']['name'] for t in study_tree['technicals'] if 'uuid' not in t.keys()]}."
                    f"Proceed with the uploading?")
                if not approve_differences:
                    return error(
                        f"Upload of already existing Study {s.name} has been aborted by the user"
                    )

            # upload the study
            upload_study(
                study_tree, url, headers, certfile, certpwd, chunk_size, IP_ADDR
            )
    except (
        RequestMethodError,
        ResourceCreationException,
        ResourceRetrievingException,
        ResourceAssignationException,
        ResourceModificationException,
        UploadInitException,
        UploadException,
        GeodataException,
        RelationshipException,
    ) as exc:
        return error(exc)

    return None
