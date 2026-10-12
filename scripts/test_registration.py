"""Contract test for deterministic Matrix virtual room/ghost namespaces."""
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
GEN=ROOT/"scripts/generate_registration.py"

class RegistrationTests(unittest.TestCase):
    def test_namespace_and_tokens(self):
        with tempfile.TemporaryDirectory() as temporary:
            base=Path(temporary)
            env=base/"bridge.env"
            result=base/"registration.yaml"
            env.write_text(
                "BRIDGE_MATRIX_AS_TOKEN="+"a"*64+"\n"
                "BRIDGE_MATRIX_HS_TOKEN="+"b"*64+"\n"
                "BRIDGE_MATRIX_SERVER_NAME=example.org\n",encoding="utf-8"
            )
            subprocess.run([sys.executable,str(GEN),"--env",str(env),
                "--output",str(result),"--url","http://2bm:9001"],
                cwd=base,check=True,env={"PATH":os.environ.get("PATH","")})
            content=result.read_text(encoding="utf-8")
            self.assertIn("regex: '^#_2bm_r_[0-9a-f]{32}:example\\.org$'",content)
            self.assertIn("regex: '^@_2bm_p_[0-9a-f]{32}:example\\.org$'",content)
            self.assertIn("regex: '^@_2bm:example\\.org$'",content)
            self.assertIn("exclusive: true",content)
            if os.name=="posix":
                self.assertEqual(result.stat().st_mode & 0o777,0o600)

if __name__=="__main__":
    unittest.main()
