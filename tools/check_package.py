#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Check the actual distributable, including class visibility and UI versioning."""

import argparse
import io
import json
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parents[1]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
POM = ET.parse(ROOT / "pom.xml")
VERSION = POM.findtext("m:version", namespaces=NS)
AWS_SDK_VERSION = POM.findtext("m:properties/m:aws.sdk.version", namespaces=NS)
ARCHIVE = ROOT / "package" / "target" / f"sqs-connector-{VERSION}.zip"
PREFIX = "sqs-connector/"

# OIE supplies the SDK core, STS, S3 and HTTP implementations. Only the SQS
# service module belongs in this extension alongside its own three modules.
EXPECTED_LIBRARIES = {
    f"libs/sqs-connector-shared-{VERSION}.jar": "SHARED",
    f"libs/sqs-connector-server-{VERSION}.jar": "SERVER",
    f"libs/sqs-connector-client-{VERSION}.jar": "CLIENT",
    f"libs/sqs-{AWS_SDK_VERSION}.jar": "SERVER",
}


def require(condition, message):
    if not condition:
        raise SystemExit(f"Package verification failed: {message}")


def verify_archive(archive_path):
    with zipfile.ZipFile(archive_path) as archive:
        entries = archive.namelist()
        names = set(entries)
        require(len(entries) == len(names), "duplicate ZIP entries")
        jar_names = {name for name in names if name.lower().endswith(".jar")}
        expected_jars = {PREFIX + path for path in EXPECTED_LIBRARIES}
        require(jar_names == expected_jars,
                f"JAR payload differs: missing={sorted(expected_jars - jar_names)}, "
                f"unexpected={sorted(jar_names - expected_jars)}")

        license_text = (ROOT / "LICENSE").read_bytes()
        require(b"Mozilla Public License Version 2.0" in license_text, "wrong plugin license")
        require(archive.read(PREFIX + "LICENSE") == license_text, "extension license differs from source")
        jar_classes = {}
        for name in sorted(jar_names):
            with zipfile.ZipFile(io.BytesIO(archive.read(name))) as jar:
                jar_classes[name] = set(jar.namelist())
                if "sqs-connector-" in Path(name).name:
                    require(jar.read("META-INF/LICENSE") == license_text,
                            f"module license differs from source: {name}")

        providers = {
            ("SERVLET_INTERFACE", "com.mirth.connect.connectors.sqs.SqsConnectorServletInterface"),
            ("SERVER_CLASS", "io.github.gibson9583.sqs.SqsConnectorServlet"),
        }
        expected_plugin = "io.github.gibson9583.sqs.SqsConnectorServicePlugin"
        for descriptor in ("plugin.xml", "source.xml", "destination.xml"):
            data = archive.read(PREFIX + descriptor)
            require(b"${" not in data, f"unfiltered token in {descriptor}")
            metadata = ET.fromstring(data)
            require(metadata.findtext("pluginVersion") == VERSION, f"wrong version in {descriptor}")
            actual_providers = {(p.get("type"), p.get("name")) for p in metadata.findall("apiProvider")}
            require(actual_providers == providers, f"API registrations differ in {descriptor}")
            libraries = metadata.findall("library")
            paths = [library.get("path") for library in libraries]
            require(len(paths) == len(set(paths)), f"duplicate library entries in {descriptor}")
            actual_libraries = {library.get("path"): library.get("type") for library in libraries}
            require(actual_libraries == EXPECTED_LIBRARIES,
                    f"library paths or scopes differ in {descriptor}: {actual_libraries}")
            for library in libraries:
                require(PREFIX + library.get("path") in names,
                        f"missing {library.get('path')} referenced by {descriptor}")
            for visibility, classname in actual_providers | {("SERVER_CLASS", expected_plugin)}:
                scopes = {"SHARED"} if visibility == "SERVLET_INTERFACE" else {"SHARED", "SERVER"}
                available = set().union(*(jar_classes[PREFIX + p.get("path")]
                                          for p in libraries if p.get("type") in scopes))
                require(classname.replace(".", "/") + ".class" in available,
                        f"{classname} is unavailable to {visibility} in {descriptor}")
            if descriptor == "plugin.xml":
                require(metadata.findtext("serverClasses/string") == expected_plugin,
                        "inspection permission service is not registered")

        web_files = {name for name in names if name.startswith(PREFIX + "webadmin/") and not name.endswith("/")}
        require(web_files == {PREFIX + "webadmin/plugin.json", PREFIX + "webadmin/web/plugin.js"},
                f"unexpected web payload: {sorted(web_files)}")
        manifest = json.loads(archive.read(PREFIX + "webadmin/plugin.json"))
        require(manifest["version"] == VERSION, "web version does not match engine plugin version")
        require(b"${message.encodedData}" in archive.read(PREFIX + "webadmin/web/plugin.js"),
                "message replacement token was lost from web bundle")
        for name, classes in jar_classes.items():
            require(not any(token in name for token in ("junit", "mockito", "byte-buddy", "xstream")),
                    f"test dependency shipped: {name}")
            if "sqs-connector-" in Path(name).name:
                require(not any(c.endswith("Test.class") for c in classes), f"tests shipped: {name}")

        return len(jar_classes)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", nargs="?", type=Path, default=ARCHIVE)
    args = parser.parse_args()
    library_count = verify_archive(args.archive)
    print(f"Verified {args.archive.name}: descriptors, API visibility, {library_count} libraries, and web bundle.")


if __name__ == "__main__":
    main()
