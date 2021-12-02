import re
import tempfile
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

POST = "post"
PUT = "put"


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
                timeout=30,
                cert=cert,
            )

        if method == PUT:
            return requests.put(
                url,
                data=data,
                headers=headers,
                timeout=30,
                cert=cert,
            )

        return requests.get(
            url,
            headers=headers,
            timeout=30,
            cert=cert,
        )


def error(text: str, r: Optional[requests.Response] = None) -> None:
    if r:
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
    file: Path,
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, List[str]]]]:
    with open(file) as f:

        header: List[str] = []
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
                continue

            # pedigree_id = line[0]
            individual_id = line[1]
            father = line[2]
            mother = line[3]
            sex = line[4]

            if sex == "1" or sex == "M":
                sex = "male"
            elif sex == "2" or sex == "F":
                sex = "female"

            properties = {}
            properties["name"] = individual_id
            properties["sex"] = sex

            age = get_value("age", header, line)
            if age is not None:
                properties["age"] = int(age)

            # birth_place = get_value("birthplace", header, line)
            # if birth_place is not None:
            #     properties["birth_place"] = birth_place

            hpo = get_value("hpo", header, line)
            if hpo is not None:
                hpo_list = hpo.split(",")
                properties["hpo"] = hpo_list

            phenotypes.append(properties)

            # parse relationships
            relationships[individual_id] = []

            if father and father != "-":
                relationships[individual_id].append(father)

            if mother and mother != "-":
                relationships[individual_id].append(mother)

            # if the phenotype has not relationships, delete the key
            if not relationships[individual_id]:
                del relationships[individual_id]

    return phenotypes, relationships


def parse_file_tech(file: Path) -> None:

    with open(file) as f:

        header: List[str] = []
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
                continue

            name = line[0]
            date = line[1]
            platform = line[2]
            # Not used
            # reference = line[3]
            kit = line[4]

            properties = {}
            properties["name"] = name
            if date and date != "-":
                properties["sequencing_date"] = date_from_string(date)
            else:
                properties["sequencing_date"] = ""
            properties["platform"] = platform
            properties["enrichment_kit"] = kit
            error(f"TODO: create {name} with props = {properties}")

            value = get_value("dataset", header, line)
            if value is not None:
                dataset_list = value.split(",")
                for dataset_name in dataset_list:
                    error(f"TODO: connect {name} to {dataset_name}")


def version_callback(value: bool) -> None:
    if value:
        typer.echo("NIG Upload version: 0.1")
        raise typer.Exit()


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
                if dat.is_file() and dat.name.endswith(".fastq.gz"):
                    study_tree["datasets"].setdefault(d.name, [])
                    study_tree["datasets"][d.name].append(dat)

    if not study_tree["datasets"]:
        return error(f"No files found for upload in: {study}")

    pedigree = study.joinpath("pedigree.txt")
    phenotypes_uuid: Dict[str, str] = {}
    if pedigree.is_file():
        phenotypes_list, relationships = parse_file_ped(pedigree)
        # validate phenotypes: check if they are associated to an existing dataset
        for p in phenotypes_list:
            if p["name"] not in study_tree["datasets"].keys():
                # phenotype has to have the same name of the dataset to be associated
                return error(
                    f"Phenotype {p['name']} is not related to any existing dataset"
                )
            # add a key in phenotypes_uuid dictionary
            phenotypes_uuid[p["name"]] = ""
        # check if relationships are valid
        for key, value in relationships.items():
            for el in value:
                if el not in phenotypes_uuid.keys():
                    return error(
                        f"Relationship between {key} and {el}: Phenotype {el} does not exists"
                    )

        study_tree["phenotypes"] = phenotypes_list
        study_tree["relationships"] = relationships

    technical = study.joinpath("technical.txt")
    if technical.is_file():
        parse_file_tech(technical)
        study_tree["technicals"] = technical

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
        for phenotype in study_tree["phenotypes"]:
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
            error("TODO add geodata for birthplace")
            error("TODO add hpo list")

    # create phenotypes relationships
    if study_tree["relationships"]:
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

                success(f"Succesfully created relationship between {son} and {parent}")

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

            chunksize = 16 * 1024 * 1024  # 16 mb
            range_start = 0

            with open(file, "rb") as f:
                with typer.progressbar(length=filesize, label="Uploading") as progress:
                    while True:
                        read_data = f.read(chunksize)
                        if not read_data:
                            break  # done
                        if range_start != 0:
                            range_start += 1
                        range_max = range_start + chunksize
                        if range_max > filesize:
                            range_max = filesize
                        headers[
                            "Content-Range"
                        ] = f"bytes {range_start}-{range_max}/{filesize}"
                        r = request(
                            method=PUT,
                            url=f"{url}api/dataset/{uuid}/files/upload/{filename}",
                            headers=headers,
                            certfile=certfile,
                            certpwd=certpwd,
                            data=read_data,
                        )

                        if r.status_code != 206:
                            if r.status_code == 200:
                                # upload is complete
                                progress.update(filesize)
                                break
                            return error("Upload Failed", r)
                        progress.update(chunksize)
                        # update the range variable
                        range_start += chunksize
                if r.status_code != 200:
                    return error("Upload Failed", r)

                success("Upload finished succesfully")

        error(f"TODO: set UPLOAD COMPLETE to {dataset_name}")

    return None
