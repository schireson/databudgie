from databudgie.utils import join_paths


class Test_join_paths:
    def test_one_component(self):
        path = join_paths("/first")
        assert path == "/first"

    def test_first_component_absolute_path(self):
        path = join_paths("/first", "/2nd")
        assert path == "/first/2nd"

    def test_non_first_component_absolute_path(self):
        path = join_paths("first", "/2nd/")
        assert path == "first/2nd/"

    def test_bad_first_component(self):
        path = join_paths(None, "/first")
        assert path == "/first"

    def test_no_components(self):
        path = join_paths(None)
        assert path == ""
