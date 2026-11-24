"""Exercise rehearsal checkout against a local Git remote, without VM/network access."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parent / 'deployment/checkout-cosiflow.sh'


class CheckoutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.home = root / 'guest'
        self.home.mkdir()
        self.remote = root / 'remote'
        self.remote.mkdir()
        self.env = dict(os.environ, HOME=str(self.home), GIT_CONFIG_NOSYSTEM='1',
                        GIT_CONFIG_GLOBAL=os.devnull)
        self.git('init', str(self.remote))
        self.git('-C', str(self.remote), 'config', 'user.email', 'test@example.invalid')
        self.git('-C', str(self.remote), 'config', 'user.name', 'Checkout test')
        (self.remote / 'source.txt').write_text('original\n')
        self.git('-C', str(self.remote), 'add', 'source.txt')
        self.git('-C', str(self.remote), 'commit', '-m', 'Candidate')
        self.sha = self.git('-C', str(self.remote), 'rev-parse', 'HEAD').strip()
        self.git('-C', str(self.remote), 'tag', '-a', 'v1.0.0', '-m', 'Release')

    def git(self, *args):
        return subprocess.check_output(['git', *args], env=self.env, text=True,
                                       stderr=subprocess.DEVNULL)

    def checkout(self, ref):
        return subprocess.run(['bash', str(SCRIPT), ref, str(self.remote)],
                              env=self.env, capture_output=True, text=True)

    def test_commit_checkout_is_detached_recorded_and_repeatable(self):
        for _ in range(2):
            result = self.checkout(self.sha)
            self.assertEqual(result.returncode, 0, result.stderr)
        target = self.home / 'cosiflow'
        self.assertEqual(self.git('-C', str(target), 'rev-parse', 'HEAD').strip(), self.sha)
        symbolic = subprocess.run(['git', '-C', str(target), 'symbolic-ref', '-q', 'HEAD'],
                                  env=self.env, capture_output=True)
        self.assertNotEqual(symbolic.returncode, 0)
        record = (self.home / '.config/cosiflow-kubernetes/source-ref.txt').read_text()
        self.assertIn('commit=' + self.sha, record)

    def test_annotated_version_tag_resolves_to_commit(self):
        result = self.checkout('v1.0.0')
        self.assertEqual(result.returncode, 0, result.stderr)
        record = (self.home / '.config/cosiflow-kubernetes/source-ref.txt').read_text()
        self.assertIn('ref=v1.0.0\n', record)
        self.assertIn('commit=' + self.sha, record)

    def test_dirty_checkout_is_preserved(self):
        self.assertEqual(self.checkout(self.sha).returncode, 0)
        source = self.home / 'cosiflow/source.txt'
        source.write_text('local change\n')
        self.assertNotEqual(self.checkout(self.sha).returncode, 0)
        self.assertEqual(source.read_text(), 'local change\n')

    def test_different_commit_is_not_replaced(self):
        self.assertEqual(self.checkout(self.sha).returncode, 0)
        (self.remote / 'source.txt').write_text('new candidate\n')
        self.git('-C', str(self.remote), 'commit', '-am', 'Next candidate')
        new_sha = self.git('-C', str(self.remote), 'rev-parse', 'HEAD').strip()
        self.assertNotEqual(self.checkout(new_sha).returncode, 0)
        self.assertEqual(self.git('-C', str(self.home / 'cosiflow'), 'rev-parse', 'HEAD').strip(), self.sha)

    def test_archive_directory_and_branch_ref_are_rejected(self):
        target = self.home / 'cosiflow'
        target.mkdir()
        (target / 'source.txt').write_text('snapshot\n')
        self.assertNotEqual(self.checkout(self.sha).returncode, 0)
        self.assertEqual((target / 'source.txt').read_text(), 'snapshot\n')
        self.assertEqual(self.checkout('main').returncode, 2)


if __name__ == '__main__':
    unittest.main()
