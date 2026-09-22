#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Check the actual distributable, including class visibility and UI versioning."""

import io
import json
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parents[1]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
VERSION = ET.parse(ROOT / "pom.xml").findtext("m:version", namespaces=NS)
ARCHIVE = ROOT / "package" / "target" / f"sqs-connector-{VERSION}.zip"
PREFIX = "sqs-connector/"


def require(condition, message):
    if not condition:
        raise SystemExit(f"Package verification failed: {message}")


with zipfile.ZipFile(ARCHIVE) as archive:
    names = set(archive.namelist())
    license_text = (ROOT / "LICENSE").read_bytes()
    require(b"Mozilla Public License Version 2.0" in license_text, "wrong plugin license")
    require(archive.read(PREFIX + "LICENSE") == license_text, "extension license differs from source")
    jar_classes = {}
    for name in names:
        if name.endswith(".jar"):
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

print(f"Verified {ARCHIVE.name}: descriptors, API visibility, {len(jar_classes)} libraries, and web bundle.")
