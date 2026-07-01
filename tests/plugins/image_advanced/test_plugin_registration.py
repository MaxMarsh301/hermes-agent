"""Registration and manifest tests for the bundled image_advanced plugin."""
from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
PLUGIN_DIR = REPO_ROOT / "plugins" / "image_advanced"


class TestManifest:
    def test_plugin_directory_exists(self):
        assert PLUGIN_DIR.is_dir()
        assert (PLUGIN_DIR / "plugin.yaml").exists()
        assert (PLUGIN_DIR / "__init__.py").exists()

    def test_manifest_fields(self):
        data = yaml.safe_load((PLUGIN_DIR / "plugin.yaml").read_text())
        assert data["name"] == "image_advanced"
        assert data["version"]
        assert "Advanced image workflow tool" in data["description"]


class TestRegister:
    def test_register_wires_expected_tool(self):
        import plugins.image_advanced as plugin

        calls = []

        class _Ctx:
            def register_tool(self, **kw):
                calls.append(kw)

        plugin.register(_Ctx())

        assert len(calls) == 1
        assert calls[0]["name"] == "image_generate_advanced"
        assert calls[0]["toolset"] == "image_gen"
        assert calls[0]["schema"]["name"] == "image_generate_advanced"
        assert callable(calls[0]["handler"])


class TestDiscovery:
    def test_plugin_is_discovered_but_not_loaded_without_opt_in(self, tmp_path, monkeypatch):
        from hermes_cli import plugins as plugins_mod

        home = tmp_path / ".hermes"
        home.mkdir()
        monkeypatch.setenv("HERMES_HOME", str(home))
        monkeypatch.setattr(Path, "home", lambda: tmp_path)

        manager = plugins_mod.PluginManager()
        manager.discover_and_load()

        loaded = manager._plugins.get("image_advanced")
        assert loaded is not None
        assert loaded.enabled is False
        assert "not enabled" in (loaded.error or "").lower()
