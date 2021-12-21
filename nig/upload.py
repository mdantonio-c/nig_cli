import json
import re
import tempfile
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
    """Exception for invalid sex"""


class PhenotypeNameException(Exception):
    """Exception for phenotypes that have names not related to an existing dataset"""


class RelationshipException(Exception):
    """Exception for a relationship between non existing phenotypes"""


class TechnicalMalformedException(Exception):
    """Exception for malformed technical files"""


class UnknownPlatformException(Exception):
    """Exception for unknown platform for technicals"""


class TechnicalAssociationException(Exception):
    """Exception for technicals with a non existing dataset associated"""


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

    with pfx_to_pem(certfile, certpwd) as cert:
        if method == POST:
            return requests.post(
                url,
                data=data,
                headers=headers,
                timeout=15,
                cert=cert,
            )

        if method == PUT:
            return requests.put(
                url,
                data=data,
                headers=headers,
                timeout=15,
                cert=cert,
            )

        if method == PATCH:
            return requests.patch(
                url,
                data=data,
                headers=headers,
                timeout=15,
                cert=cert,
            )

        if method == GET:
            return requests.get(
                url,
                headers=headers,
                timeout=15,
                cert=cert,
            )

        # if hasn't returned yet is because the method is unknown
        raise RequestMethodError(f"method {method} not allowed")


def error(text: str, r: Optional[requests.Response] = None) -> None:
    if r is not None:
        text += f". Status: {r.status_code}, response: {get_response(r)}"
    typer.secho(text, fg=typer.colors.RED)
    return None


def success(text: str) -> None:
    typer.secho(text, fg=typer.colors.GREEN)
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
        typer.echo("NIG Upload version: 0.3")
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


@app.command()
def upload(
    study: Path = typer.Argument(..., help="Path to the study"),
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

    if not url.startswith("https:"):
        url = f"https://{url}"
    if not url.endswith("/"):
        url = f"{url}/"

    if not certfile.exists():
        return error(f"Certificate not found: {certfile}")

    if chunk_size > 16:
        return error(f"The specified chunk size is too large: {chunk_size}")

    # check if the input file exists
    if not study.exists():
        return error(f"The specified study does not exists: {study}")

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
            if (
                study_tree["datasets"].get(d.name)
                and len(study_tree["datasets"][d.name]) > 2
            ):
                # the dataset is invalid because contains too many fastq
                return error(
                    f"Dataset {d.name} contains too many fastq files: max allowed files are 2 per dataset"
                )

    if not study_tree["datasets"]:
        return error(f"No files found for upload in: {study}")

    pedigree = study.joinpath("pedigree.txt")
    phenotypes_uuid: Dict[str, str] = {}
    technicals_uuid: Dict[str, str] = {}
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
            return error(exc)

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
            return error(exc)
        study_tree["technicals"] = technicals_list

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
            return error("Study creation failed", r)

        success(f"Succesfully created study {study_name}")

        study_uuid = r.json()

        # create phenotypes
        if study_tree["phenotypes"]:
            # get geodata list
            r = request(
                method=POST,
                url=f"{url}api/study/{study_uuid}/phenotypes",
                headers=headers,
                certfile=certfile,
                certpwd=certpwd,
                data={"get_schema": True},
            )
            if r.status_code != 200:
                return error("Can't retrieve geodata list", r)
            for el in r.json():
                if el["key"] == "birth_place":
                    geodata = el["options"]
                    break
            for phenotype in study_tree["phenotypes"]:
                # get the birth_place
                if phenotype.get("birth_place_name"):
                    for geo_id, name in geodata.items():
                        if name == phenotype["birth_place_name"]:
                            phenotype["birth_place"] = geo_id
                            break
                    if "birth_place" not in phenotype.keys():
                        return error(
                            f"Error for phenotype {phenotype['name']}: {phenotype['birth_place_name']} birth place not found"
                        )

                    # delete birth_place_name key
                    del phenotype["birth_place_name"]

                r = request(
                    method=POST,
                    url=f"{url}api/study/{study_uuid}/phenotypes",
                    headers=headers,
                    certfile=certfile,
                    certpwd=certpwd,
                    data=phenotype,
                )
                if r.status_code != 200:
                    return error("Phenotype creation failed", r)

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
                        return error("Phenotype relationship failed", r)

                    success(
                        f"Succesfully created relationship between {son} and {parent}"
                    )

        # create technicals
        if study_tree["technicals"]:
            for technical in study_tree["technicals"]:
                r = request(
                    method=POST,
                    url=f"{url}api/study/{study_uuid}/technicals",
                    headers=headers,
                    certfile=certfile,
                    certpwd=certpwd,
                    data=technical["properties"],
                )
                if r.status_code != 200:
                    return error("Technical creation failed", r)

                success(
                    f"Succesfully created technical {technical['properties']['name']}"
                )

                # add the uuid in the technical uuid dictionary
                technicals_uuid[technical["properties"]["name"]] = r.json()

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
                return error("Dataset creation failed", r)

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
                    return error("Can't assign a phenotype to the dataset", r)

                success(f"Succesfully assigned phenotype to dataset {dataset_name}")

            #  connect the technical to the dataset
            if study_tree["technicals"]:
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
                        return error("Can't assign a technical to the dataset", r)

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
                    return error("Can't start the upload", r)

                success("Upload succesfully initialized")

                chunk = chunk_size * 1024 * 1024
                range_start = -1
                prev_position = 0

                with open(file, "rb") as f:
                    start = datetime.now()
                    with typer.progressbar(
                        length=filesize, label="Uploading"
                    ) as progress:
                        while True:

                            prev_position = f.tell()
                            read_data = f.read(chunk)
                            # No more data read from the file
                            if not read_data:
                                break

                            range_start += 1

                            range_max = min(range_start + chunk, filesize)

                            content_range = (
                                f"bytes {range_start}-{range_max}/{filesize}"
                            )
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
                                return error("Upload Failed", r)
                            progress.update(chunk)
                            # update the range variable
                            range_start += chunk

                    end = datetime.now()
                    seconds = (end - start).seconds or 1

                    t = get_time(seconds)
                    s = get_speed(filesize / seconds)
                    if r.status_code != 200:
                        return error(f"Upload Failed in {t} ({s})", r)

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
                return error("Can't set the status to the dataset", r)

            success(f"Succesfully set UPLOAD COMPLETE to {dataset_name}")

    except RequestMethodError as exc:
        return error(exc)

    return None
