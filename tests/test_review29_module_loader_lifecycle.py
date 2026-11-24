import shlex
import subprocess
from unittest import skipUnless

from tests import test_review13_module_loader_safety as review13


@skipUnless(review13.HAS_PYYAML, "Review 29 lifecycle tests require PyYAML")
class ModuleLoaderLifecycleTests(review13.ModuleLoaderShellTests):
    """Review 29 contracts built on the Review 13 fake-Docker harness."""

    def add_environment_files(self, *names):
        for name in names:
            (self.module / "env" / f"requirements-{name}.txt").write_text(
                "", encoding="utf-8"
            )

    def multi_environment_config(self, *, alpha=True, beta=True):
        return f"""
            install_mode: environment
            paths:
              dags: src/dags
              pipeline: src/pipeline
            environments:
              alpha:
                requirements: env/requirements-alpha.txt
                venv_path: /home/gamma/envs/alpha
                enabled: {str(alpha).lower()}
              beta:
                requirements: env/requirements-beta.txt
                venv_path: /home/gamma/envs/beta
                enabled: {str(beta).lower()}
        """

    def test_documented_invocation_installs_enabled_environments(self):
        self.add_environment_files("alpha", "beta")
        self.write_config(self.multi_environment_config())

        result = self.run_loader("safe-module", "install", "-e")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Environment plan: alpha beta", result.stdout)
        self.assertIn("2 environment(s) created and verified", result.stdout)
        calls = self.docker_calls()
        self.assertTrue(any("/home/gamma/envs/alpha/bin/python" in call for call in calls))
        self.assertTrue(any("/home/gamma/envs/beta/bin/python" in call for call in calls))

    def test_explicit_selection_uses_one_config_snapshot(self):
        self.add_environment_files("alpha", "beta")
        self.write_config(self.multi_environment_config(alpha=False, beta=False))

        result = self.run_loader("safe-module", "install", "-E", "beta,alpha")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Environment plan: beta alpha", result.stdout)
        config_calls = [
            call
            for call in self.docker_calls()
            if "/home/gamma/venv/bin/python" in call
            and "/home/gamma/module_config.py" in call
        ]
        self.assertEqual(len(config_calls), 1, config_calls)
        self.assertIn("snapshot", config_calls[0])

    def test_all_selection_installs_every_configured_environment(self):
        self.add_environment_files("alpha", "beta")
        self.write_config(self.multi_environment_config(alpha=False, beta=False))

        result = self.run_loader("safe-module", "install", "-E", "all")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Environment plan: alpha beta", result.stdout)
        self.assertIn("2 environment(s) created and verified", result.stdout)

    def test_empty_effective_selection_fails_before_mutation(self):
        self.add_environment_files("alpha", "beta")
        self.write_config(self.multi_environment_config(alpha=False, beta=False))

        result = self.run_loader("safe-module", "install", "-e")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no environments were selected", result.stderr)
        self.assertNotIn("ready!", result.stdout)
        self.assert_no_mutation()

    def test_failed_removal_is_not_reported_as_success(self):
        self.write_config("install_mode: none\n")

        result = self.run_loader(
            "safe-module", "remove", extra_env={"FAKE_FAIL_COMMAND": "rm"}
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("DAGs link removed", result.stdout)
        self.assertNotIn("Module safe-module removed", result.stdout)

    def test_environment_failure_cleans_partial_state_and_fails_closed(self):
        self.add_environment_files("alpha", "beta")
        self.write_config(self.multi_environment_config())

        result = self.run_loader(
            "safe-module",
            "install",
            "-E",
            "alpha",
            extra_env={"FAKE_FAIL_COMMAND": "/home/gamma/envs/alpha/bin/python"},
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Partial state was removed", result.stderr)
        self.assertNotIn("ready!", result.stdout)
        recursive_removals = [
            call for call in self.docker_calls() if "rm" in call and "-rf" in call
        ]
        self.assertGreaterEqual(len(recursive_removals), 2)

    def test_link_activation_and_image_failures_propagate(self):
        scenarios = (
            ("ln", "install_mode: none\n"),
            ("chmod", self.multi_environment_config(alpha=True, beta=False)),
            (
                "build",
                """
                install_mode: container
                paths:
                  dags: src/dags
                  pipeline: src/pipeline
                  images: env
                """,
            ),
        )
        self.add_environment_files("alpha", "beta")
        for command, config in scenarios:
            with self.subTest(command=command):
                self.log_path.unlink(missing_ok=True)
                (self.root / "links.log").unlink(missing_ok=True)
                self.write_config(config)
                result = self.run_loader(
                    "safe-module",
                    "install",
                    extra_env={"FAKE_FAIL_COMMAND": command},
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("ready!", result.stdout)

    def test_activation_helper_is_valid_and_sources_in_clean_bash(self):
        self.add_environment_files("alpha", "beta")
        self.write_config(self.multi_environment_config(alpha=True, beta=False))

        result = self.run_loader("safe-module", "install", "-e")

        self.assertEqual(result.returncode, 0, result.stderr)
        captured = self.root / "activate.sh"
        syntax = subprocess.run(["bash", "-n", str(captured)], check=False)
        self.assertEqual(syntax.returncode, 0)

        fake_venv = self.root / "fake venv"
        (fake_venv / "bin").mkdir(parents=True)
        (fake_venv / "bin" / "python").write_text("", encoding="utf-8")
        (fake_venv / "bin" / "python").chmod(0o755)
        (fake_venv / "bin" / "activate").write_text(
            f"VIRTUAL_ENV={shlex.quote(str(fake_venv))}\n"
            f"PATH={shlex.quote(str(fake_venv / 'bin'))}:$PATH\n"
            "export VIRTUAL_ENV PATH\n",
            encoding="utf-8",
        )
        local_helper = self.root / "activate-local.sh"
        local_helper.write_text(
            captured.read_text(encoding="utf-8").replace(
                "/home/gamma/envs/alpha/bin/activate",
                shlex.quote(str(fake_venv / "bin" / "activate")),
            ),
            encoding="utf-8",
        )
        source = subprocess.run(
            [
                "bash",
                "--noprofile",
                "--norc",
                "-c",
                'source "$1" >/dev/null && printf "%s\\n%s\\n" "$VIRTUAL_ENV" "$(command -v python)"',
                "bash",
                str(local_helper),
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(source.returncode, 0, source.stderr)
        self.assertEqual(
            source.stdout.splitlines(),
            [str(fake_venv), str(fake_venv / "bin" / "python")],
        )

    def test_install_and_remove_are_repeatable(self):
        self.add_environment_files("alpha", "beta")
        self.write_config(self.multi_environment_config())

        for action in ("install", "update", "remove", "remove"):
            with self.subTest(action=action):
                result = self.run_loader("safe-module", action)
                self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    import unittest

    unittest.main()
