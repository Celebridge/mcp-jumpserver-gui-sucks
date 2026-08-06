from importlib.metadata import version

from mcp_jumpserver_gui_sucks import __version__


def test_runtime_version_matches_installed_package_metadata() -> None:
    assert __version__ == version("mcp-jumpserver-gui-sucks")
