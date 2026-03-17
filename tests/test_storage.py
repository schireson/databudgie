from databudgie.storage import FileSelectionStrategy, LocalStorage


def test_get_file_content_with_dotslash_prefix(tmp_path, monkeypatch):
    """get_file_content should find files when the path has a leading './'."""
    monkeypatch.chdir(tmp_path)

    subdir = tmp_path / "public.store"
    subdir.mkdir()
    (subdir / "2021-04-26T09:00:00.csv").write_bytes(b"id,name\n1,foo\n")

    storage = LocalStorage()
    strategy = FileSelectionStrategy.use_filename_strategy

    # The leading "./" caused match_path to fail because os.scandir returns
    # paths without the "./" prefix, so the regex never matched.
    result = storage.get_file_content("./public.store/2021-04-26T09:00:00.csv", strategy)

    assert result is not None
