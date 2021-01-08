# Copyright © 2020 Hashmap, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os

from hdm.core.orchestrator.declared_orchestrator import DeclaredOrchestrator
from tests.hdm_test_case import HDMTestCase


class TestDeclaredOrchestrator(HDMTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.create_test_staging_folder()
        self._path = os.path.dirname(os.path.realpath(__file__))

    def test_cotr_1(self):
        os.environ['HDM_MANIFEST'] = os.path.join(os.getenv('HDM_MANIFESTS'), 'manual_example_1.yml')

        orchestrator = DeclaredOrchestrator()

        self.assertIsInstance(orchestrator, DeclaredOrchestrator)

    def test_cotr_2(self):
        os.environ['HDM_MANIFEST'] = os.path.join(os.getenv('HDM_MANIFESTS'), 'manual_example_2.yml')

        orchestrator = DeclaredOrchestrator()

        self.assertIsInstance(orchestrator, DeclaredOrchestrator)
