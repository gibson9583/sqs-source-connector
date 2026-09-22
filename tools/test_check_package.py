# SPDX-License-Identifier: MPL-2.0
"""Mutation tests for the distributable guard; no Maven build or AWS access needed."""

import io
import json
import unittest
import warnings
import xml.etree.ElementTree as ET
import zipfile

import check_package


PREFIX = "sqs-connector/"
DESCRIPTORS = ("plugin.xml", "source.xml", "destination.xml")


def zip_bytes(entries):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for name, contents in entries:
            archive.writestr(name, contents)
    return output.getvalue()


class PackageVerificationTest(unittest.TestCase):
    def setUp(self):
        version = check_package.VERSION
        self.sqs_path = PREFIX + f"libs/sqs-{check_package.AWS_SDK_VERSION}.jar"
        self.shared_path = PREFIX + f"libs/sqs-connector-shared-{version}.jar"
        self.server_path = PREFIX + f"libs/sqs-connector-server-{version}.jar"
        client_path = PREFIX + f"libs/sqs-connector-client-{version}.jar"
        self.license = (check_package.ROOT / "LICENSE").read_bytes()
        self.entries = {PREFIX + "LICENSE": self.license}
        module_classes = {
            self.shared_path: ["com/mirth/connect/connectors/sqs/SqsConnectorServletInterface.class"],
            self.server_path: ["io/github/gibson9583/sqs/SqsConnectorServlet.class",
                               "io/github/gibson9583/sqs/SqsConnectorServicePlugin.class"],
            client_path: [],
        }
        for path, classes in module_classes.items():
            self.entries[path] = zip_bytes([("META-INF/LICENSE", self.license)] +
                                           [(name, b"class fixture") for name in classes])
        self.entries[self.sqs_path] = zip_bytes([
            ("software/amazon/awssdk/services/sqs/SqsClient.class", b"class fixture")])
        libraries = [(self.shared_path, "SHARED"), (self.server_path, "SERVER"),
                     (client_path, "CLIENT"), (self.sqs_path, "SERVER")]
        for descriptor in DESCRIPTORS:
            metadata = ET.Element("pluginMetaData" if descriptor == "plugin.xml" else "connectorMetaData")
            ET.SubElement(metadata, "pluginVersion").text = version
            if descriptor == "plugin.xml":
                ET.SubElement(ET.SubElement(metadata, "serverClasses"), "string").text = (
                    "io.github.gibson9583.sqs.SqsConnectorServicePlugin")
            ET.SubElement(metadata, "apiProvider", type="SERVLET_INTERFACE",
                          name="com.mirth.connect.connectors.sqs.SqsConnectorServletInterface")
            ET.SubElement(metadata, "apiProvider", type="SERVER_CLASS",
                          name="io.github.gibson9583.sqs.SqsConnectorServlet")
            for path, scope in libraries:
                ET.SubElement(metadata, "library", path=path.removeprefix(PREFIX), type=scope)
            self.entries[PREFIX + descriptor] = ET.tostring(metadata)
        self.entries[PREFIX + "webadmin/plugin.json"] = json.dumps({"version": version}).encode()
        self.entries[PREFIX + "webadmin/web/plugin.js"] = b"const template = '${message.encodedData}';"

    def verify(self, entries=None):
        payload = zip_bytes((entries if entries is not None else self.entries).items())
        return check_package.verify_archive(io.BytesIO(payload))

    def test_valid_four_jar_distribution(self):
        self.assertEqual(self.verify(), 4)

    def test_unreferenced_redundant_jar_is_rejected(self):
        # Inspect every location, not just JARs registered by descriptors.
        for path in ("libs/sts-2.15.28.jar", "webadmin/netty.jar", "extra/SDK.JAR"):
            with self.subTest(path=path):
                entries = dict(self.entries)
                entries[PREFIX + path] = zip_bytes([])
                with self.assertRaisesRegex(SystemExit, "JAR payload differs.*unexpected="):
                    self.verify(entries)

    def test_missing_sqs_jar_is_rejected(self):
        del self.entries[self.sqs_path]
        with self.assertRaisesRegex(SystemExit, "JAR payload differs.*missing=.*sqs-"):
            self.verify()

    def test_renamed_old_module_jar_is_rejected(self):
        self.entries[PREFIX + "libs/sqs-connector-server-old.jar"] = self.entries.pop(self.server_path)
        with self.assertRaisesRegex(SystemExit, "JAR payload differs"):
            self.verify()

    def test_duplicate_zip_entries_are_rejected(self):
        for path in (self.sqs_path, PREFIX + "plugin.xml"):
            with self.subTest(path=path), warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                payload = zip_bytes(list(self.entries.items()) + [(path, self.entries[path])])
                with self.assertRaisesRegex(SystemExit, "duplicate ZIP entries"):
                    check_package.verify_archive(io.BytesIO(payload))

    def test_each_descriptor_rejects_invalid_library_registrations(self):
        for descriptor in DESCRIPTORS:
            for mutation in ("wrong_scope", "missing", "stale", "duplicate", "extra"):
                with self.subTest(descriptor=descriptor, mutation=mutation):
                    entries = dict(self.entries)
                    key = PREFIX + descriptor
                    metadata = ET.fromstring(entries[key])
                    sqs_library = metadata.findall("library")[-1]
                    if mutation == "wrong_scope":
                        sqs_library.set("type", "CLIENT")
                    elif mutation == "missing":
                        metadata.remove(sqs_library)
                    elif mutation == "stale":
                        sqs_library.set("path", "libs/sqs-old.jar")
                    elif mutation == "duplicate":
                        ET.SubElement(metadata, "library", sqs_library.attrib)
                    else:
                        ET.SubElement(metadata, "library", path="libs/sts-old.jar", type="SERVER")
                    entries[key] = ET.tostring(metadata)
                    error = "duplicate library entries" if mutation == "duplicate" else "library paths or scopes differ"
                    with self.assertRaisesRegex(SystemExit, error + " in " + descriptor):
                        self.verify(entries)

    def test_client_and_shared_scopes_are_enforced(self):
        for original_scope in ("CLIENT", "SHARED"):
            with self.subTest(scope=original_scope):
                entries = dict(self.entries)
                key = PREFIX + "source.xml"
                metadata = ET.fromstring(entries[key])
                library = next(p for p in metadata.findall("library") if p.get("type") == original_scope)
                library.set("type", "SERVER")
                entries[key] = ET.tostring(metadata)
                with self.assertRaisesRegex(SystemExit, "library paths or scopes differ in source.xml"):
                    self.verify(entries)

    def test_api_class_visibility_is_still_checked(self):
        self.entries[self.shared_path] = zip_bytes([("META-INF/LICENSE", self.license)])
        with self.assertRaisesRegex(SystemExit, "SqsConnectorServletInterface is unavailable"):
            self.verify()

    def test_module_licenses_are_still_checked(self):
        self.entries[self.server_path] = zip_bytes([("META-INF/LICENSE", b"incorrect")])
        with self.assertRaisesRegex(SystemExit, "module license differs from source"):
            self.verify()

    def test_web_version_is_still_checked(self):
        self.entries[PREFIX + "webadmin/plugin.json"] = b'{"version":"old"}'
        with self.assertRaisesRegex(SystemExit, "web version does not match"):
            self.verify()

    def test_test_classes_are_still_rejected(self):
        with zipfile.ZipFile(io.BytesIO(self.entries[self.server_path])) as jar:
            entries = [(name, jar.read(name)) for name in jar.namelist()]
        self.entries[self.server_path] = zip_bytes(entries + [("example/AccidentallyShippedTest.class", b"test")])
        with self.assertRaisesRegex(SystemExit, "tests shipped"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
