from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

from .support import REPO_ROOT, load_script


class HTTPException(Exception):
    def __init__(self, status_code=500):
        super().__init__(f"HTTP {status_code}")
        self.code = status_code


class Response:
    def __init__(self):
        self.headers = {}


class Blueprint:
    def __init__(self, *_args, **_kwargs):
        pass

    def record_once(self, function):
        return function


def _identity_decorator(*_args, **_kwargs):
    def decorate(function):
        return function

    return decorate


def _abort(status_code):
    raise HTTPException(status_code)


def _url_for(endpoint, filepath=""):
    route = {
        "HEASARCExplorerView.preview_file": "preview",
        "HEASARCExplorerView.image_file": "image",
        "HEASARCExplorerView.download_file": "download",
    }.get(endpoint, "")
    return f"/heasarcbrowser/{route}/{filepath}" if route else "/heasarcbrowser/"


def load_data_explorer():
    airflow = ModuleType("airflow")
    airflow_plugins = ModuleType("airflow.plugins_manager")
    airflow_models = ModuleType("airflow.models")
    airflow_plugins.AirflowPlugin = type("AirflowPlugin", (), {})
    airflow_models.BaseOperator = type("BaseOperator", (), {})

    flask = ModuleType("flask")
    flask.Blueprint = Blueprint
    flask.send_from_directory = lambda *_args, **_kwargs: Response()
    flask.jsonify = lambda value: value
    flask.abort = _abort
    flask.url_for = _url_for

    flask_appbuilder = ModuleType("flask_appbuilder")
    flask_appbuilder.BaseView = type(
        "BaseView",
        (),
        {"render_template": lambda self, template, **context: (template, context)},
    )
    flask_appbuilder.expose = _identity_decorator

    jinja2 = ModuleType("jinja2")
    jinja2.Environment = type("Environment", (), {})

    shared_auth = ModuleType("shared_auth")
    shared_auth.ACTION_READ = "can_read"
    shared_auth.SCIENTIFIC_DATA = "COSIflow Scientific Data"
    shared_auth.require_cosiflow_permission = _identity_decorator

    shared_ui = ModuleType("shared_ui")
    shared_ui.add_shared_templates = lambda blueprint: blueprint

    werkzeug = ModuleType("werkzeug")
    werkzeug_exceptions = ModuleType("werkzeug.exceptions")
    werkzeug_exceptions.HTTPException = HTTPException

    modules = {
        "airflow": airflow,
        "airflow.plugins_manager": airflow_plugins,
        "airflow.models": airflow_models,
        "flask": flask,
        "flask_appbuilder": flask_appbuilder,
        "jinja2": jinja2,
        "shared_auth": shared_auth,
        "shared_ui": shared_ui,
        "werkzeug": werkzeug,
        "werkzeug.exceptions": werkzeug_exceptions,
    }
    with patch.dict(sys.modules, modules):
        return load_script(
            "plugins/data_explorer/data_explorer_plugin.py",
            "cosiflow_review19_data_explorer_under_test",
        )


DATA_EXPLORER = load_data_explorer()
PLUGIN_PATH = REPO_ROOT / "plugins" / "data_explorer" / "data_explorer_plugin.py"
TEMPLATE_PATH = REPO_ROOT / "plugins" / "data_explorer" / "templates" / "explorer.html"
NODE_EXECUTABLE = os.environ.get("REVIEW19_NODE") or shutil.which("node")


class DataExplorerServerSecurityTests(unittest.TestCase):
    hostile_detail = "/srv/private/data/<img src=x onerror=alert(1)>"

    def setUp(self):
        self.view = DATA_EXPLORER.HEASARCExplorerView()

    def assert_sanitized_failure(self, result, public_message, logs):
        self.assertEqual(result, (public_message, 500))
        rendered = repr(result)
        self.assertNotIn(self.hostile_detail, rendered)
        self.assertNotIn("Traceback", rendered)
        self.assertIn(self.hostile_detail, "\n".join(logs.output))

    def test_home_failure_is_sanitized_and_logged_server_side(self):
        with patch.object(
            DATA_EXPLORER.os,
            "listdir",
            side_effect=RuntimeError(self.hostile_detail),
        ), self.assertLogs(DATA_EXPLORER.logger, level="ERROR") as logs:
            result = self.view.explorer_home()

        self.assert_sanitized_failure(
            result, "Unable to load the Data Explorer.", logs
        )

    def test_folder_failure_is_sanitized_and_logged_server_side(self):
        with patch.object(
            DATA_EXPLORER,
            "_resolve_data_path",
            side_effect=RuntimeError(self.hostile_detail),
        ), self.assertLogs(DATA_EXPLORER.logger, level="ERROR") as logs:
            result = self.view.explorer_folder("hostile")

        self.assert_sanitized_failure(
            result, "Unable to load the requested folder.", logs
        )

    def test_download_failure_is_sanitized_and_logged_server_side(self):
        with patch.object(
            DATA_EXPLORER,
            "_resolve_data_path",
            side_effect=RuntimeError(self.hostile_detail),
        ), self.assertLogs(DATA_EXPLORER.logger, level="ERROR") as logs:
            result = self.view.download_file("hostile")

        self.assert_sanitized_failure(
            result, "Unable to download the requested file.", logs
        )

    def test_image_failure_is_sanitized_and_logged_server_side(self):
        with patch.object(
            DATA_EXPLORER,
            "_resolve_data_path",
            side_effect=RuntimeError(self.hostile_detail),
        ), self.assertLogs(DATA_EXPLORER.logger, level="ERROR") as logs:
            result = self.view.image_file("hostile.png")

        self.assert_sanitized_failure(
            result, "Unable to load the requested image.", logs
        )

    def test_preview_failure_is_sanitized_and_logged_server_side(self):
        with patch.object(
            DATA_EXPLORER,
            "_resolve_data_path",
            side_effect=RuntimeError(self.hostile_detail),
        ), self.assertLogs(DATA_EXPLORER.logger, level="ERROR") as logs:
            payload, status = self.view.preview_file("hostile")

        self.assertEqual(status, 500)
        self.assertEqual(payload, {"error": "Unable to load the file preview."})
        self.assertNotIn(self.hostile_detail, repr(payload))
        self.assertIn(self.hostile_detail, "\n".join(logs.output))

    def test_intentional_http_errors_are_not_rewritten(self):
        for method_name in (
            "explorer_folder",
            "download_file",
            "image_file",
            "preview_file",
        ):
            with self.subTest(method=method_name), patch.object(
                DATA_EXPLORER,
                "_resolve_data_path",
                side_effect=HTTPException(403),
            ):
                with self.assertRaises(HTTPException) as caught:
                    getattr(self.view, method_name)("denied")
                self.assertEqual(caught.exception.code, 403)

    def test_image_response_retains_isolation_headers(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            image = Path(temporary_directory) / "plot.png"
            image.write_bytes(b"not-a-real-png")
            response = Response()
            with patch.object(DATA_EXPLORER, "DL0_FOLDER", temporary_directory), patch.object(
                DATA_EXPLORER,
                "send_from_directory",
                return_value=response,
            ):
                result = self.view.image_file("plot.png")

        self.assertIs(result, response)
        self.assertEqual(result.headers["X-Content-Type-Options"], "nosniff")
        self.assertIn("sandbox", result.headers["Content-Security-Policy"])

    def test_svg_is_not_embedded_as_an_inline_data_url(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            image = Path(temporary_directory) / "hostile.svg"
            image.write_text("<svg onload='alert(1)'></svg>", encoding="utf-8")
            with patch.object(DATA_EXPLORER, "DL0_FOLDER", temporary_directory):
                payload = self.view.preview_file("hostile.svg")

        self.assertEqual(payload["content_type"], "image_metadata")
        self.assertEqual(payload["mime_type"], "image/svg+xml")
        self.assertNotIn("content", payload)
        self.assertEqual(payload["image_url"], "/heasarcbrowser/image/hostile.svg")

    def test_supported_raster_preview_contains_valid_base64(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            image = Path(temporary_directory) / "plot.png"
            image.write_bytes(b"safe-raster-content")
            with patch.object(DATA_EXPLORER, "DL0_FOLDER", temporary_directory):
                payload = self.view.preview_file("plot.png")

        self.assertEqual(payload["content_type"], "image")
        self.assertEqual(payload["mime_type"], "image/png")
        self.assertRegex(
            payload["content"],
            r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$",
        )

    def test_source_contains_no_raw_exception_response_contract(self):
        source = PLUGIN_PATH.read_text(encoding="utf-8")
        self.assertNotIn("traceback.format_exc", source)
        self.assertNotIn("import traceback", source)
        self.assertNotRegex(source, r"jsonify\([^\n]*str\(e\)")
        self.assertNotRegex(source, r"return f[\"'][^\n]*\{e\}")


class DataExplorerDomSecurityTests(unittest.TestCase):
    @staticmethod
    def _browser_script():
        source = TEMPLATE_PATH.read_text(encoding="utf-8")
        match = re.search(
            r"<!-- JavaScript for file preview functionality -->\s*<script>(.*?)</script>",
            source,
            re.DOTALL,
        )
        if match is None:
            raise AssertionError("Data Explorer preview script not found")
        script = match.group(1)
        routes = {
            "preview_file": "/heasarcbrowser/preview/__FILEPATH__",
            "image_file": "/heasarcbrowser/image/__FILEPATH__",
            "download_file": "/heasarcbrowser/download/__FILEPATH__",
        }
        for endpoint, route in routes.items():
            pattern = (
                r"\{\{\s*url_for\(\s*'HEASARCExplorerView\."
                + endpoint
                + r"'.*?\)\s*\|\s*tojson\s*\}\}"
            )
            script, count = re.subn(
                pattern,
                json.dumps(route),
                script,
                count=1,
                flags=re.DOTALL,
            )
            if count != 1:
                raise AssertionError(f"Could not replace {endpoint} URL template")
        return script

    @unittest.skipUnless(NODE_EXECUTABLE, "Node.js is required for the DOM security test")
    def test_hostile_dom_payloads_and_urls_are_rejected_or_rendered_as_text(self):
        preview_script = self._browser_script()
        harness = r"""
const assert = require('assert');

class Element {
    constructor(tagName) {
        this.tagName = tagName.toUpperCase();
        this.children = [];
        this.attributes = {};
        this.style = {};
        this.className = '';
        this.listeners = {};
        this._text = '';
        this.classList = { add() {}, remove() {} };
    }
    append(...nodes) {
        for (const node of nodes) {
            if (node && node.tagName === '#FRAGMENT') {
                this.children.push(...node.children);
            } else {
                this.children.push(node);
            }
        }
    }
    replaceChildren(...nodes) {
        this.children = [];
        this.append(...nodes);
    }
    setAttribute(name, value) { this.attributes[name] = String(value); }
    addEventListener(name, listener) { this.listeners[name] = listener; }
    scrollTo() {}
    set textContent(value) { this._text = String(value); this.children = []; }
    get textContent() { return this._text + this.children.map(child => child.textContent || '').join(''); }
}

const previewContent = new Element('div');
const previewTitle = new Element('h3');
const previewFilename = new Element('p');
const document = {
    addEventListener() {},
    createElement(tagName) { return new Element(tagName); },
    createDocumentFragment() { return new Element('#fragment'); },
    createTextNode(value) { const node = new Element('#text'); node.textContent = value; return node; },
    querySelector(selector) {
        return {
            '.preview-content': previewContent,
            '.preview-title': previewTitle,
            '.preview-filename': previewFilename,
        }[selector] || null;
    },
    querySelectorAll() { return []; },
};
const window = {
    location: { origin: 'https://cosiflow.example', href: 'https://cosiflow.example/heasarcbrowser/' },
    open() { throw new Error('window.open must not run during rendering'); },
};
const fetch = () => Promise.reject(new Error('<img src=x onerror=alert(1)> /srv/private'));

function allTags(node) {
    return [node.tagName, ...node.children.flatMap(allTags)];
}
function findTag(node, tagName) {
    if (node.tagName === tagName) return node;
    for (const child of node.children) {
        const found = findTag(child, tagName);
        if (found) return found;
    }
    return null;
}

/* REVIEW19_ASSERTIONS */
const hostile = '<img src=x onerror="globalThis.pwned=true"><script>alert(1)</script>${7*7}';

displayPreview({ error: hostile }, 'ignored');
assert.strictEqual(previewContent.textContent, hostile);
assert.ok(!allTags(previewContent).includes('IMG'));
assert.ok(!allTags(previewContent).includes('SCRIPT'));

displayPreview({ content_type: 'text', content: hostile }, 'ignored');
assert.strictEqual(previewContent.textContent, hostile);
assert.ok(!allTags(previewContent).includes('IMG'));

assert.strictEqual(
    validateRouteUrl('/heasarcbrowser/image/safe.png', imageUrlTemplate),
    'https://cosiflow.example/heasarcbrowser/image/safe.png'
);
for (const candidate of [
    'javascript:alert(1)',
    'data:text/html,<script>alert(1)</script>',
    'https://evil.example/heasarcbrowser/image/x.png',
    '//evil.example/heasarcbrowser/image/x.png',
    '/other/image/x.png',
    '/heasarcbrowser/image/x.png?redirect=https://evil.example',
    '/heasarcbrowser/image/x.png#fragment',
]) {
    assert.throws(() => validateRouteUrl(candidate, imageUrlTemplate));
}

assert.strictEqual(validateInlineImagePayload({ mime_type: 'image/png', content: 'aGVsbG8=' }), 'data:image/png;base64,aGVsbG8=');
assert.throws(() => validateInlineImagePayload({ mime_type: 'image/svg+xml', content: 'PHN2Zz4=' }));
assert.throws(() => validateInlineImagePayload({ mime_type: 'image/png', content: hostile }));

displayPreview({
    content_type: 'image_metadata',
    image_url: '/heasarcbrowser/image/safe.png',
    preview_title: hostile,
    caption: hostile,
    metadata: { [hostile]: hostile },
    size: 12,
}, 'ignored');
assert.strictEqual(previewTitle.textContent, hostile);
assert.ok(!allTags(previewContent).includes('IMG'));
assert.ok(!allTags(previewContent).includes('SCRIPT'));
const safeLink = findTag(previewContent, 'A');
assert.strictEqual(safeLink.href, 'https://cosiflow.example/heasarcbrowser/image/safe.png');

displayPreview({
    content_type: 'image',
    image_url: 'javascript:alert(1)',
    mime_type: 'image/png',
    content: 'aGVsbG8=',
}, hostile);
assert.strictEqual(previewContent.textContent, 'The preview response is invalid.');
assert.ok(!allTags(previewContent).includes('IMG'));

displayPreview({
    content_type: 'image',
    image_url: '/heasarcbrowser/image/safe.png',
    mime_type: 'image/png',
    content: 'aGVsbG8=',
}, hostile);
const safeImage = findTag(previewContent, 'IMG');
assert.ok(safeImage);
assert.strictEqual(safeImage.alt, hostile);
assert.strictEqual(safeImage.attributes.onerror, undefined);

loadFilePreview('safe.txt', hostile);
setImmediate(() => {
    assert.strictEqual(previewContent.textContent, 'Unable to load the file preview.');
    assert.ok(!allTags(previewContent).includes('IMG'));
});
"""
        setup, assertions = harness.split("/* REVIEW19_ASSERTIONS */", 1)
        completed = subprocess.run(
            [NODE_EXECUTABLE, "-"],
            input=setup + "\n" + preview_script + "\n" + assertions,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(
            completed.returncode,
            0,
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}",
        )

    def test_template_has_no_inner_html_sink(self):
        source = TEMPLATE_PATH.read_text(encoding="utf-8")
        self.assertNotIn("innerHTML", source)
        self.assertNotIn("escapeHtml", source)
        self.assertIn("replaceChildren", source)
        self.assertIn("textContent", source)


if __name__ == "__main__":
    unittest.main()
