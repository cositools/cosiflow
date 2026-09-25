import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
HELPER_PATH = REPO_ROOT / "env" / "module_config.py"
LOADER_PATH = REPO_ROOT / "env" / "hot_load_module.sh"
HAS_PYYAML = importlib.util.find_spec("yaml") is not None


def load_helper():
    spec = importlib.util.spec_from_file_location("review13_module_config", HELPER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless(HAS_PYYAML, "Review 13 parser tests require PyYAML")
class ModuleConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.helper = load_helper()

    def parse(self, content):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "module.config.yaml"
            path.write_text(textwrap.dedent(content), encoding="utf-8")
            return self.helper.load_config(path)

    def test_valid_yaml_preserves_quoted_hash_and_types(self):
        config = self.parse(
            """
            install_mode: environment
            paths:
              dags: "src/dags with space"
              pipeline: src/pipeline
              images: env
            environments:
              analysis:
                requirements: env/requirements.txt
                venv_path: /home/gamma/envs/analysis
                enabled: true
                description: "analysis # primary"
                python_version: "3.12"
            default_environment: analysis
            """
        )
        self.assertEqual(config["install_mode"], "environment")
        self.assertEqual(config["paths"]["dags"], "src/dags with space")
        self.assertEqual(
            config["environments"]["analysis"]["description"], "analysis # primary"
        )
        self.assertIs(config["environments"]["analysis"]["enabled"], True)

    def test_duplicate_keys_are_rejected(self):
        with self.assertRaisesRegex(self.helper.ConfigError, "duplicate YAML key"):
            self.parse("install_mode: none\ninstall_mode: both\n")

    def test_malformed_yaml_is_rejected(self):
        with self.assertRaisesRegex(self.helper.ConfigError, "invalid YAML"):
            self.parse("paths: [unterminated\n")

    def test_wrong_types_and_unknown_modes_are_rejected(self):
        invalid_documents = (
            "install_mode: [none]\n",
            "install_mode: unsupported\n",
            "paths:\n  dags: [src/dags]\n",
            "environments:\n  analysis:\n    requirements: req.txt\n    enabled: yes\n",
        )
        for document in invalid_documents:
            with self.subTest(document=document):
                with self.assertRaises(self.helper.ConfigError):
                    self.parse(document)

    def test_control_characters_are_rejected(self):
        with self.assertRaisesRegex(self.helper.ConfigError, "control characters"):
            self.parse(
                """
                environments:
                  analysis:
                    requirements: env/requirements.txt
                    description: "analysis\\x1b[2J"
                """
            )

    def test_destructive_paths_are_rejected(self):
        unsafe_paths = (
            "/",
            "/home/gamma",
            "/home/gamma/envs",
            "/home/gamma/envs-backup/analysis",
            "/home/gamma/envs/../analysis",
            "/home/gamma/envs/analysis;touch-pwned",
            "/home/gamma/envs/$(touch-pwned)",
        )
        for unsafe_path in unsafe_paths:
            with self.subTest(path=unsafe_path):
                with self.assertRaises(self.helper.ConfigError):
                    self.parse(
                        f"""
                        environments:
                          analysis:
                            requirements: env/requirements.txt
                            venv_path: {unsafe_path!r}
                        """
                    )

    def test_default_venv_path_is_confined(self):
        config = self.parse(
            """
            environments:
              analysis:
                requirements: env/requirements.txt
            """
        )
        self.assertEqual(
            config["environments"]["analysis"]["venv_path"],
            "/home/gamma/envs/analysis",
        )

    def test_duplicate_environment_targets_are_rejected(self):
        with self.assertRaisesRegex(self.helper.ConfigError, "must be distinct"):
            self.parse(
                """
                environments:
                  analysis:
                    requirements: env/requirements.txt
                    venv_path: /home/gamma/envs/shared
                  calibration:
                    requirements: env/requirements.txt
                    venv_path: /home/gamma/envs/shared
                """
            )

    def test_nested_environment_targets_are_rejected(self):
        with self.assertRaisesRegex(self.helper.ConfigError, "must not overlap"):
            self.parse(
                """
                environments:
                  analysis:
                    requirements: env/requirements.txt
                    venv_path: /home/gamma/envs/analysis
                  nested:
                    requirements: env/requirements.txt
                    venv_path: /home/gamma/envs/analysis/nested
                """
            )

    def test_unsafe_environment_identifiers_are_rejected(self):
        for name in ("..", "bad/name", "bad name", "bad;name", "$(touch-pwned)"):
            with self.subTest(name=name):
                with self.assertRaises(self.helper.ConfigError):
                    self.parse(
                        f"""
                        environments:
                          {name!r}:
                            requirements: env/requirements.txt
                        """
                    )


@unittest.skipUnless(HAS_PYYAML, "Review 13 shell tests require PyYAML")
class ModuleLoaderShellTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / "workspace"
        self.cosiflow = self.workspace / "cosiflow"
        self.env_dir = self.cosiflow / "env"
        self.env_dir.mkdir(parents=True)
        shutil.copy2(LOADER_PATH, self.env_dir / "hot_load_module.sh")
        shutil.copy2(HELPER_PATH, self.env_dir / "module_config.py")
        self.modules_root = self.cosiflow / "modules-pool"
        self.module = self.modules_root / "safe-module"
        (self.module / "src" / "dags").mkdir(parents=True)
        (self.module / "src" / "pipeline").mkdir(parents=True)
        (self.module / "env").mkdir(parents=True)
        (self.module / "env" / "requirements.txt").write_text("", encoding="utf-8")
        (self.module / "env" / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")

        self.fake_bin = self.root / "bin"
        self.fake_bin.mkdir()
        self.log_path = self.root / "docker.log"
        fake_docker = self.fake_bin / "docker"
        fake_docker.write_text(
            textwrap.dedent(
                """\
                #!/bin/bash
                printf '%s\\t' "$@" >> "$FAKE_DOCKER_LOG"
                printf '\\n' >> "$FAKE_DOCKER_LOG"
                if [ "$1" != "exec" ]; then
                    exit 0
                fi
                shift
                if [ "$1" = "-u" ]; then
                    shift 2
                fi
                shift
                command="$1"
                shift
                if [ "$command" = "/home/gamma/venv/bin/python" ]; then
                    shift
                    translated=()
                    for argument in "$@"; do
                        case "$argument" in
                            /home/gamma/airflow/modules_pool/*)
                                argument="$FAKE_WORKSPACE/${argument#/home/gamma/airflow/modules_pool/}"
                                ;;
                        esac
                        translated+=("$argument")
                    done
                    exec "$FAKE_TEST_PYTHON" "$FAKE_CONFIG_HELPER" "${translated[@]}"
                fi
                if [ "$command" = "realpath" ]; then
                    candidate="${@: -1}"
                    if [ -n "$FAKE_REALPATH_RESULT" ]; then
                        printf '%s\\n' "$FAKE_REALPATH_RESULT"
                    else
                        printf '%s\\n' "$candidate"
                    fi
                    exit 0
                fi
                exit 0
                """
            ),
            encoding="utf-8",
        )
        fake_docker.chmod(0o755)

    def write_config(self, content):
        path = self.module / "env" / "safe.config.yaml"
        path.write_text(textwrap.dedent(content), encoding="utf-8")
        return path

    def run_loader(self, *arguments, extra_env=None):
        environment = os.environ.copy()
        environment.update(
            {
                "PATH": f"{self.fake_bin}:/usr/bin:/bin",
                "FAKE_DOCKER_LOG": str(self.log_path),
                "FAKE_WORKSPACE": str(self.modules_root),
                "FAKE_TEST_PYTHON": sys.executable,
                "FAKE_CONFIG_HELPER": str(self.env_dir / "module_config.py"),
            }
        )
        if extra_env:
            environment.update(extra_env)
        return subprocess.run(
            ["bash", str(self.env_dir / "hot_load_module.sh"), *arguments],
            cwd=self.env_dir,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def docker_calls(self):
        if not self.log_path.exists():
            return []
        return [line.rstrip("\n").split("\t")[:-1] for line in self.log_path.read_text().splitlines()]

    def assert_no_mutation(self):
        mutating_commands = {"rm", "ln", "cp", "chmod", "build", "rmi"}
        for call in self.docker_calls():
            self.assertTrue(mutating_commands.isdisjoint(call), call)

    def test_malformed_yaml_fails_before_mutation(self):
        self.write_config("paths: [unterminated\n")
        result = self.run_loader("safe-module", "install")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("configuration", result.stderr.lower())
        self.assert_no_mutation()

    def test_unsafe_venv_path_fails_before_mutation(self):
        self.write_config(
            """
            install_mode: environment
            environments:
              analysis:
                requirements: env/requirements.txt
                venv_path: /home/gamma
                enabled: true
            """
        )
        result = self.run_loader("safe-module", "install")
        self.assertNotEqual(result.returncode, 0)
        self.assert_no_mutation()

    def test_shell_payload_in_venv_path_is_not_executed(self):
        marker = self.root / "pwned"
        self.write_config(
            f"""
            install_mode: environment
            environments:
              analysis:
                requirements: env/requirements.txt
                venv_path: "/home/gamma/envs/analysis;touch {marker}"
                enabled: true
            """
        )
        result = self.run_loader("safe-module", "install")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(marker.exists())
        self.assert_no_mutation()

    def test_shell_payload_in_selected_environment_is_rejected(self):
        marker = self.root / "selected-pwned"
        self.write_config(
            """
            install_mode: environment
            environments:
              analysis:
                requirements: env/requirements.txt
                venv_path: /home/gamma/envs/analysis
                enabled: true
            """
        )
        result = self.run_loader(
            "safe-module",
            "install",
            "-E",
            f"analysis;touch-{marker.name}",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(marker.exists())
        self.assert_no_mutation()

    def test_requirements_outside_module_fail_before_mutation(self):
        outside = self.workspace / "outside-requirements.txt"
        outside.write_text("", encoding="utf-8")
        self.write_config(
            f"""
            install_mode: environment
            environments:
              analysis:
                requirements: {str(outside)!r}
                venv_path: /home/gamma/envs/analysis
                enabled: true
            """
        )
        result = self.run_loader("safe-module", "install")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must stay below", result.stderr)
        self.assert_no_mutation()

    def test_dag_pipeline_and_image_paths_outside_module_fail_before_mutation(self):
        outside = self.workspace / "outside-directory"
        outside.mkdir()
        for field in ("dags", "pipeline", "images"):
            with self.subTest(field=field):
                self.log_path.unlink(missing_ok=True)
                paths = {"dags": "src/dags", "pipeline": "src/pipeline", "images": "env"}
                paths[field] = str(outside)
                path_lines = "\n".join(f"  {key}: {value!r}" for key, value in paths.items())
                self.write_config(f"install_mode: none\npaths:\n{path_lines}\n")
                result = self.run_loader("safe-module", "install")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must stay below", result.stderr)
                self.assert_no_mutation()

    def test_missing_dockerfile_fails_before_link_mutation(self):
        (self.module / "env" / "Dockerfile").unlink()
        self.write_config(
            """
            install_mode: container
            paths:
              dags: src/dags
              pipeline: src/pipeline
              images: env
            """
        )
        result = self.run_loader("safe-module", "install")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Dockerfile not found", result.stderr)
        self.assert_no_mutation()

    def test_symlink_escape_fails_before_mutation(self):
        self.write_config(
            """
            install_mode: none
            environments:
              analysis:
                requirements: env/requirements.txt
                venv_path: /home/gamma/envs/link/analysis
            """
        )
        result = self.run_loader(
            "safe-module",
            "install",
            extra_env={"FAKE_REALPATH_RESULT": "/home/gamma/outside/analysis"},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assert_no_mutation()

    def test_explicit_config_outside_module_is_rejected_without_docker(self):
        outside = self.workspace / "outside.config.yaml"
        outside.write_text("install_mode: none\n", encoding="utf-8")
        result = self.run_loader("safe-module", "install", "-c", str(outside))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must stay below", result.stderr)
        self.assertEqual(self.docker_calls(), [])

    def test_module_traversal_is_rejected_without_docker(self):
        result = self.run_loader("../safe-module", "install")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Module name", result.stderr)
        self.assertEqual(self.docker_calls(), [])

    def test_cli_path_with_control_character_is_rejected_before_mutation(self):
        self.write_config("install_mode: none\n")
        result = self.run_loader("safe-module", "install", "-d", "src/dags\nother")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("control characters", result.stderr)
        self.assert_no_mutation()

    def test_valid_install_and_update_preserve_path_with_spaces_as_one_argument(self):
        (self.module / "src" / "dags with space").mkdir()
        self.write_config(
            """
            install_mode: none
            paths:
              dags: "src/dags with space"
              pipeline: src/pipeline
              images: env
            """
        )
        expected = "/home/gamma/airflow/modules_pool/safe-module/src/dags with space"
        for action in ("install", "update"):
            with self.subTest(action=action):
                self.log_path.unlink(missing_ok=True)
                result = self.run_loader("safe-module", action)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertTrue(any(expected in call for call in self.docker_calls()))

    def test_remove_uses_confined_recursive_delete(self):
        self.write_config(
            """
            install_mode: environment
            environments:
              analysis:
                requirements: env/requirements.txt
                venv_path: /home/gamma/envs/analysis
            """
        )
        result = self.run_loader("safe-module", "remove")
        self.assertEqual(result.returncode, 0, result.stderr)
        recursive_removals = [
            call for call in self.docker_calls() if "rm" in call and "-rf" in call
        ]
        self.assertEqual(len(recursive_removals), 1)
        self.assertIn("/home/gamma/envs/analysis", recursive_removals[0])
        self.assertNotIn("/home/gamma/envs", recursive_removals[0][-1:])

    def test_remove_rejects_missing_module_before_mutation(self):
        shutil.rmtree(self.module)
        result = self.run_loader("safe-module", "remove")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Module directory not found", result.stderr)
        self.assertEqual(self.docker_calls(), [])

    def test_remove_preflights_configured_paths_before_mutation(self):
        outside = self.workspace / "outside-directory"
        outside.mkdir()
        self.write_config(
            f"""
            install_mode: environment
            paths:
              dags: {str(outside)!r}
              pipeline: src/pipeline
            environments:
              analysis:
                requirements: env/requirements.txt
                venv_path: /home/gamma/envs/analysis
            """
        )
        result = self.run_loader("safe-module", "remove")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must stay below", result.stderr)
        self.assert_no_mutation()


if __name__ == "__main__":
    unittest.main()
