from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_cosiflow_branding_is_packaged_in_the_airflow_image() -> None:
    dockerfile = (PROJECT_ROOT / "env" / "Dockerfile.airflow").read_text()
    installer = (PROJECT_ROOT / "env" / "install_airflow_branding.py").read_text()
    navbar = (PROJECT_ROOT / "env" / "branding" / "navbar.html").read_text()
    footer = (PROJECT_ROOT / "env" / "branding" / "footer.html").read_text()
    logo = PROJECT_ROOT / "env" / "branding" / "cosiflow-logo.webp"

    assert "COPY --chown=${UID}:${GID} branding /home/gamma/branding/" in dockerfile
    assert "RUN python /home/gamma/install_airflow_branding.py" in dockerfile
    assert "COSIflow" in navbar
    assert "cosiflow/cosiflow-logo.webp" in navbar
    assert 'html[data-color-scheme="dark"] .cosiflow-brand-logo' in navbar
    assert "COSIflow version:" in footer
    assert "Powered by Airflow v:" in footer
    assert "{{ airflow_version }}" in footer
    assert "cosiflow/cosiflow-logo.webp" in installer
    assert 'type="image/webp"' in installer
    assert logo.is_file()
    assert logo.read_bytes().startswith(b"RIFF")
