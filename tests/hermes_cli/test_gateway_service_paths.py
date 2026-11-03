from unittest.mock import patch


def test_service_path_prefers_detected_dot_venv(tmp_path):
    """Service PATH should use the detected venv dir, not a hardcoded venv/ path."""
    dot_venv = tmp_path / ".venv"
    (dot_venv / "bin").mkdir(parents=True)

    from hermes_cli.gateway import _build_service_path_dirs

    with (
        patch("hermes_cli.gateway._detect_venv_dir", return_value=dot_venv),
        patch("hermes_cli.gateway.get_hermes_home", return_value=tmp_path / ".hermes"),
    ):
        dirs = _build_service_path_dirs(project_root=tmp_path)

    assert str(dot_venv / "bin") in dirs
    assert str(tmp_path / "venv" / "bin") not in dirs


def test_service_path_skips_nonexistent_node_modules(tmp_path):
    """Service PATH should not include node_modules/.bin if it doesn't exist."""
    from hermes_cli.gateway import _build_service_path_dirs
    with patch("hermes_cli.gateway.get_hermes_home", return_value=tmp_path / ".hermes"):
        dirs = _build_service_path_dirs(project_root=tmp_path)
    node_modules_bin = str(tmp_path / "node_modules" / ".bin")
    assert node_modules_bin not in dirs


def test_service_path_includes_node_modules_when_present(tmp_path):
    """Service PATH should include node_modules/.bin when it exists."""
    nm_bin = tmp_path / "node_modules" / ".bin"
    nm_bin.mkdir(parents=True)
    from hermes_cli.gateway import _build_service_path_dirs
    with patch("hermes_cli.gateway.get_hermes_home", return_value=tmp_path / ".hermes"):
        dirs = _build_service_path_dirs(project_root=tmp_path)
    assert str(nm_bin) in dirs


def test_service_path_includes_hermes_home_node_modules(tmp_path):
    """Service PATH should include ~/.hermes/node_modules/.bin when it exists."""
    hermes_nm = tmp_path / ".hermes" / "node_modules" / ".bin"
    hermes_nm.mkdir(parents=True)
    from hermes_cli.gateway import _build_service_path_dirs
    with patch("hermes_cli.gateway.get_hermes_home", return_value=tmp_path / ".hermes"):
        dirs = _build_service_path_dirs(project_root=tmp_path)
    assert str(hermes_nm) in dirs
